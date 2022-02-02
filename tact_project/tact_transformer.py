import os
import sys

import string
import re
from datetime import datetime
from csv import DictReader
from csv import DictWriter
from pathlib import Path
from pathlib import PurePosixPath
import importlib

from utils import str_to_decimal
from utils import multiple_doi
from utils import normalized_date
import lib.utils as utils
from tact_db_utils import init_database
from tact_db_utils import insert_tact_publisher_reports
from tact_db_utils import find_tact_publisher_reports_by_id
from tact_db_utils import find_tact_publisher_reports_by_publisher

publishers = [
        "ACM",
        "CoB",
        "CSP",
        "CUP",
        "Elsevier",
        "JMIR",
        "PLOS",
        "PNAS",
        "TRS",
        "Springer",
        ]


output_fieldnames = [
        "Publisher",
        "DOI",
        "Article Title",
        "Corresponding Author",
        "Corresponding Author Email",
        "UC Institution",
        "Institution Identifier",
        "Document Type",
        "Eligible",
        "Inclusion Date",
        "UC Approval Date",
        "Article Access Type",
        "Article License",
        "Journal Name",
        "ISSN/eISSN",
        "Journal Access Type",
        "Journal Subject",
        "Grant Participation",
        "Funder Information",
        "Full Coverage Reason",
        "Original APC (USD)",
        "Contractual APC (USD)",
        "Library APC Portion (USD)",
        "Author APC Portion (USD)",
        "Payment Note",
        "CDL Notes",
        "License Chosen",
        "Journal Bucket",
        "Agreement Manager Profile Name",
        "Publisher Status",
        ]

open_access_publication_titles = [
        "Disease Models Mechanisms",
        "Biology Open",
        "ACM Transactions on Architecture and Code Optimization",
        "ACM Transactions on Human Robot Interaction",
        "ACM/IMS Transactions on Data Science",
        "DGOV Research and Practice",
        "Digital Government Research and Practice",
        "Digital Threats Research and Practice",
        "PACM on Programming Languages",
        "Proceedings of the ACM on Programming Languages",
        "Transactions on Architecture and Code Optimization",
        "Transactions on Data Science",
        "Transactions on Human Robot Interaction",
        "TACO",
        "THRI",
        "TDS",
        "DGOV",
        "DTRAP",
        "PACMPL",
        ]

institution_id = {
        "UC Santa Cruz": "8787",
        "UC San Francisco": "8785",
        "UC Davis": "8789",
        "UC San Diego": "8784",
        "UC Berkeley": "1438",
        }

def define_variables(publisher):
    publisher = publisher.lower()
    mapper = importlib.import_module("{}_mapper".format(publisher))

    source_fieldnames = mapper.source_fieldnames
    mapping_function = getattr(mapper, "source_to_output_mapping")
    transform_function = globals()["transform_{}".format(publisher)]

    return source_fieldnames, mapping_function, transform_function

def transform(publisher, input_filename):

    source_fieldnames, mapping_function, transform_function = define_variables(publisher)

    input_rows = get_input_rows(input_filename, source_fieldnames)
    output_rows = map_input_to_output(input_rows, mapping_function, transform_function)
    return remove_rejected_entries(output_rows, publisher, input_filename)

def write_to_outputs(output_rows, output_filename, database):
    output_file = open(output_filename, 'w', newline='', encoding='UTF-8')
    writer = DictWriter(output_file, fieldnames=output_fieldnames)
    writer.writeheader()

    for row in output_rows:
        writer.writerow(row)

        db_record = convert_row_to_record(row)
        insert_tact_publisher_reports(database, [db_record])

    output_file.close()

def check_file_encoding(input_filename, encoding):
    print("in decode file")
    with open(input_filename, 'r', newline='', encoding=encoding) as csvfile:
        reader = DictReader(csvfile)
        for row in reader:
            pass

def get_input_rows(input_filename, source_fieldnames):
    input_rows = []
    Encoding = '' 
    try:
        print("decode file by utf-8-sig")
        check_file_encoding(input_filename, 'utf-8-sig')
        encoding = 'utf-8-sig'
    except Exception as e:
        print("decode by utf-8-sig failed: {}".format(e))
        try:
            print("decode file by cp1252")
            check_file_encoding(input_filename, 'cp1252')
            encoding = 'cp1252'
        except Exception as e:
            print("decode file by cp1252 failed: {}".format(e))
            raise e

    with open(input_filename, 'r', newline='', encoding=encoding) as csvfile:
        reader = DictReader(csvfile)

        #next(reader, None)  # skip the headers

        i=0
        for row in reader:
            i +=1
            new_row = {}
            values = ''
            for key, val in row.items():
                if key:
                    # remove leading and trailing whitespaces and trailing newline
                    row[key] = val.rstrip("\n").strip()
                    values += val.rstrip("\n").strip()

                    # if a key has spaces and newline, create a new key
                    if key.rstrip("\n").strip() != key:
                        new_row[key.rstrip("\n").strip()] = row[key]

            if not values.strip():
                print("empty line")
                continue    # skip empty lines

            if i < 3 or i > 9975:
                print(row) 

            if new_row:
                print("new keys: {}".format(new_row))
                new_row.update(row)
                input_rows.append(new_row)
            else:
                input_rows.append(row)

    print("lines: {}".format(i))
    return input_rows

def map_input_to_output(input_rows, mapping_function, transform_function):
    """Convert input data to output data format.

    Args:
      input_rows: list of dictionary representing data entries of the input file 
      mapping_function: reference to the mapping function 
      transform_function: reference to the transorm function 

    Return: list of dictionary representing transformed data entries
    """
    output_rows = []
    for row in input_rows:
        output_row = init_output_row()
        mapping_function(row, output_row)
        transform_function(output_row)
        output_rows.append(output_row)

    return output_rows

def get_dup_doi_list(rows):
    doi_list = []
    dup_doi_list = []  # duplicated dois

    for row in rows:
        if not row['DOI'].strip():
            continue  # skip if empty

        if row['DOI'] in doi_list:
            if row['DOI'] not in dup_doi_list:
                dup_doi_list.append(row['DOI'])
        else:
            doi_list.append(row['DOI'])

    return dup_doi_list


def remove_rejected_entries(rows, publisher, input_filename):
    """Remove rejected data entries.

      - Reject the data entry if there are multiple DOIs in the DOI data field.
      - Reject all data entries which has the same DOI.
      - Reject data entries without a DOI value.
    """
    dup_doi_list = get_dup_doi_list(rows) 

    line_no = 1  # header
    modified_rows = []
    for row in rows:
        line_no += 1
        reject = False

        if not row['DOI'].strip():
            print("ERROR: No DOI: publisher: {} filename: {}, line: {}".format(publisher, input_filename, line_no))
            continue

        if row['DOI'] in dup_doi_list:
            reject = True
            print("ERROR: Duplicated DOI: {}".format(row['DOI']))
            print("INFO: publisher: {} filename: {}, line: {}".format(publisher, input_filename, line_no))
       
        if multiple_doi(row['DOI']):
            reject = True
            print("ERROR: Wrong or multiple DOIs: {}".format(row['DOI']))
            print("INFO: publisher: {} filename: {}, line: {}".format(publisher, input_filename, line_no))

        if not reject:
            modified_rows.append(row)

    return modified_rows 

def transform_acm(row):
    row['Article Title'] = normalized_article_title(row['Article Title'])
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['UC Approval Date'] = normalized_date(row['UC Approval Date'], row['DOI'])
    row['Journal Access Type'] =  normalized_journal_access_type_by_title(row['Journal Name'])

def transform_cob(row):
    row['UC Institution'] = get_institution_by_email(row['Corresponding Author Email'])
    row['Institution Identifier'] = get_institution_id_by_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Journal Access Type'] = normalized_journal_access_type_by_title(row['Journal Name']) 
    row['Grant Participation'] = normalized_grant_participation_2(row['Grant Participation']) 

def transform_csp(row):
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Institution Identifier'] = get_institution_id_by_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['UC Approval Date'] = normalized_date(row['UC Approval Date'], row['DOI'])

    if row['Grant Participation'].strip() and row['Funder Information'].strip():
        row['Funder Information'] = row['Grant Participation'].strip() + ", " + row['Funder Information'].strip()
    else:
        row['Funder Information'] = row['Grant Participation'].strip() + row['Funder Information'].strip()

    if str_to_decimal(row['Author APC Portion (USD)']) > 0:
        row['Grant Participation'] = "Yes"
    elif str_to_decimal(row['Author APC Portion (USD)']) == 0 and row['Payment Note'].lower() != "awaiting payment form":
        row['Grant Participation'] = "No"
    else:
        row['Grant Participation'] = ""


def transform_cup(row):
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Article Access Type'] = normalized_article_access_type(row['Article Access Type'])
    row['Journal Access Type'] = normalized_journal_access_type(row['Journal Access Type'])
    row['Grant Participation'] = normalized_grant_participation_2(row['Grant Participation'])
    if "I have research funds available to pay the remaining balance due (you will be asked to pay the Additional Charge on a later screen)" in row['Full Coverage Reason']:
        row['Full Coverage Reason'] = ""


def transform_elsevier(row):
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Article Access Type'] = normalized_article_access_type(row['Article Access Type'])
    row['Journal Access Type'] = normalized_journal_access_type(row['Journal Access Type'])
    row['Grant Participation'] = normalized_grant_participation(row['Grant Participation'])

def transform_plos(row):
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Journal Name'] = normalized_journal_name_plos(row['Journal Name'])
    row['Grant Participation'] = "Yes" if str_to_decimal(row['Grant Participation']) > 0 else "No"

def transform_springer(row):
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Eligible'] = "Yes" if row['Eligible'].lower() in ["approved", "opt-out"] else "No"
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['UC Approval Date'] = normalized_date(row['UC Approval Date'], row['DOI'])
    row['Article Access Type'] = normalized_article_access_type(row['Article Access Type'])
    row['Grant Participation'] = normalized_grant_participation(row['Grant Participation'])

def transform_trs(row):
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Journal Access Type'] = normalized_journal_access_type(row['Journal Access Type'])

def normalized_institution_name(name):
    """Institution Look-up:
    University of California => UC System
    University of California System => "UC System"

    University of California Division of Agriculture and Natural Resources => UC Davis
    USDA Agricultural Research Service => UC Davis

    Containing UCLA => UC Los Angeles

    University of California,Institute for Integrative Genome Biology => UC Riverside

    Department of Psychological and Brain Sciences,University of California => UC Santa Barbara
    National Center for Ecological Analysis and Synthesis => UC Santa Barbara

    Containing UCSF => UC San Francisco
    Zuckerberg San Francisco General Hospital and Trauma Center => UC San Francisco
    Gladstone Institutes => UC San Francisco
    Chao Family Comprehensive Cancer Center => UC San Francisco
    
    Lawrence Berkeley National Laboratory => LBNL
    E O Lawrence Berkeley National Laboratory => LBNL
    Lawrence Livermore National Laboratory => LLNL

    General patterns:
    University of California, Davis => "UC Davis"
    University of California - Davis => "UC Davis"
    University of California Davis => "UC Davis"
    Univeristy of California, Davis School of Veterinary Medicine => UC Davis
    University of California Davis School of Medicine => UC Davis
    University of California Berkeley School of Public Health => UC Berkeley
    University of California Irvine Medical Center => UC Irvine
    University of California - San Diego School of Medicine => UC San Diego
    """
    name = name.strip()
    if name.lower() == "University of California".lower() or name.lower() == "University of California System".lower():
        return "UC System"
    elif school_name_matches(name, "Berkeley"):
        return "UC Berkeley"
    elif school_name_matches(name, "Davis") or "Division of Agriculture and Natural Resources" in name or "USDA Agricultural Research Service" in name:
        return "UC Davis"
    elif school_name_matches(name,"Irvine"):
        return "UC Irvine"
    elif school_name_matches(name,"Los Angeles") or "UCLA" in name:
        return "UC Los Angeles"
    elif school_name_matches(name,"Merced"):
        return "UC Merced"
    elif school_name_matches(name,"Riverside") or "Institute for Integrative Genome Biology" in name:
        return "UC Riverside"
    elif school_name_matches(name,"Santa Barbara") or "Department of Psychological and Brain Sciences" in name or "National Center for Ecological Analysis and Synthesis" in name:
        return "UC Santa Barbara"
    elif school_name_matches(name,"Santa Cruz"):
        return "UC Santa Cruz"
    elif school_name_matches(name,"San Diego"):
        return "UC San Diego"
    elif school_name_matches(name,"San Francisco") or "UCSF" in name or name in ["Zuckerberg San Francisco General Hospital and Trauma Center", 
            "Gladstone Institutes", 
            "Chao Family Comprehensive Cancer Center"]:
        return "UC San Francisco"
    elif "Lawrence Berkeley National Laboratory" in name:
       return "LBNL"
    elif "Lawrence Livermore National Laboratory" in name:
        return "LLNL"
    else:
        return name


def school_name_matches(name, keyword):
    return ("University of California" in name or "UC " in name) and keyword in name

def normalized_journal_access_type_by_title(publication_title):
    """Open Access look-up based on publication title.
    Normalize publication_title to change punctuation to space, change multiple spaces to single space before match. 
    Returns:
        "Fully OA": when publication_title is listed in open_access_publication_titles.
        "Hybrid": other cases.

    """
    if normalized_publication_title(publication_title) in open_access_publication_titles:
        return "Fully OA"
    else:
        return "Hybrid"

def normalized_journal_access_type(journal_access_type):
    journal_access_type = journal_access_type.lower()
    if "hybrid" in journal_access_type:
        return "Hybrid"
    elif "gold" in journal_access_type or "pure oa" in journal_access_type: 
        return "Fully OA"
    elif journal_access_type == "no oa":
        return "Subscription"
    else:
        return ""

def normalized_article_access_type(article_access_type):
    article_access_type = article_access_type.lower()
    if article_access_type in ["hybrid open access", "full open access", "approved", "yes"]:
        return "OA"
    elif article_access_type in ["subscription", "opt-out", "no"]:
        return "Subscription"
    else:
        return ""


def normalized_publication_title(title):
    title = title.replace('-', ' ')
    normalized_title = ' '.join(word.strip(string.punctuation) for word in title.split())
    normalized_title = ' '.join(normalized_title.split())  # replace multiple spaces with single space
    return normalized_title

def normalized_article_title(title):
    # change any "\"" to ""; change any "&#34;" to ""
    normalized_title = title.replace('\\"', '').replace('&#34;', '')
    return normalized_title

def normalized_grant_participation(grant_participation):
    if grant_participation in ["Y", "Yes", "Partially Covered"]:
        return "Yes"
    elif grant_participation in ["N", "No", "Fully Covered"]:
        return "No"
    return ""

def normalized_grant_participation_2(grant_participation):
    if "I have research funds" in grant_participation:
        return "Yes"
    elif "I do not have research funds" in grant_participation:
        return "No"
    return ""

def normalized_journal_name_plos(journal_name):
    journal_name = journal_name.strip()
    # drop leading three digit numerics + space "ddd "
    if re.search(r'^\d{3} ', journal_name):
        return journal_name[4:]
    return journal_name

def get_institution_by_email(email):
    email = email.lower()
    if "ucsc.edu" in email:
        return "UC Santa Cruz"
    elif "ucsf.edu" in email:
        return "UC San Francisco"
    elif "ucdavis.edu" in email:
        return "UC Davis"
    elif "ucd.edu" in email:
        return "UC Davis"
    elif "ucsd.edu" in email:
        return "UC San Diego"
    elif "berkeley.edu" in email:
        return "UC Berkeley"
    elif "uci.edu" in email:
        return "UC Irvine"
    elif "ucr.edu" in email:
        return "UC Riverside"
    elif "ucb.edu" in email:
        return "UC Berkeley"
    elif "ucla.edu" in email:
        return "UC Los Angeles"
    elif "ucm.edu" in email:
        return "UC Merced"
    elif "ucsb.edu" in email:
        return "UC Santa Barbara"
    else:
        return ""

def get_institution_id_by_name(name):
    try:
        return institution_id[name]
    except:
        return ""

    

def test_remove_punctuation():
    title = " Thank you Human-Robot!  -- You're welcome. "
    converted = "Thank you Human Robot You're welcome"
    assert(converted == normalized_publication_title(title))

def process_one_publisher(publisher, database):
    print("Processing files from {}".format(publisher))
    publisher = publisher.strip().lower()

    input_dir = Path(os.getcwd()).joinpath("./indata/{}".format(publisher))
    output_dir = Path(os.getcwd()).joinpath("./outputs/{}".format(publisher))
    processed_dir = Path(os.getcwd()).joinpath("./processed/{}".format(publisher))
    input_files = (entry for entry in input_dir.iterdir() if entry.is_file())

    for input_file in input_files:
        file_extension = PurePosixPath(input_file).suffix
        filename_wo_ext = PurePosixPath(input_file).stem
        if file_extension == ".csv":
            print("  File: {}".format(input_file))
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S_%f')
            output_filename = output_dir.joinpath("{}_output_{}.csv".format(filename_wo_ext, timestamp))
            try:
                transformed_rows = transform(publisher, input_file)
                write_to_outputs(transformed_rows, output_filename, database)

                input_file.rename(processed_dir.joinpath(input_file.name))
                print("  Processed.")
            except Exception as e:
                print("Failed to process file: {}".format(e))


def process_all_publishers(database):
    for publisher in publishers:
        process_one_publisher(publisher, database)


def usage():
    print("Parameter error.")
    print("Usage: {} publisher_name(optional)".format(sys.argv[0]))
    print("For example: {} Elsevier".format(sys.argv[0]))
    print("For example: {}".format(sys.argv[0]))
    print("Processing files from specified publisher when publisher name is provided")
    print("Otherwise processing files from all publishers")
    print("Publisher name is case insensitive")


def get_db_conn_str():
    env="dev"
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, "config")
    CONFIG_FILE = "tact_db"
    configs= utils.get_configs_by_filename(CONFIG_PATH, CONFIG_FILE)
    return str(utils.db_connect_url(configs[env]))

def init_output_row():
    return {
        "Publisher": '',
        "DOI": '',
        "Article Title": '',
        "Corresponding Author": '',
        "Corresponding Author Email": '',
        "UC Institution": '',
        "Institution Identifier": '',
        "Document Type": '',
        "Eligible": '',
        "Inclusion Date": '',
        "UC Approval Date": '',
        "Article Access Type": '',
        "Article License": '',
        "Journal Name": '',
        "ISSN/eISSN": '',
        "Journal Access Type": '',
        "Journal Subject": '',
        "Grant Participation": '',
        "Funder Information": '',
        "Full Coverage Reason": '',
        "Original APC (USD)": 0,
        "Contractual APC (USD)": 0,
        "Library APC Portion (USD)": 0,
        "Author APC Portion (USD)": 0,
        "Payment Note": '',
        "CDL Notes": '',
        "License Chosen": '',
        "Journal Bucket": '',
        "Agreement Manager Profile Name": '',
        "Publisher Status": '',
    }


def convert_row_to_record(row):
    record = {
        "publisher": row.get("Publisher", ''),
        "doi": row.get("DOI", ''),
        "article_title": row.get("Article Title", ''),
        "corresponding_author": row.get("Corresponding Author", ''),
        "corresponding_author_email": row.get("Corresponding Author Email", ''),
        "uc_institution": row.get("UC Institution", ''),
        "institution_identifier": row.get("Institution Identifier", ''),
        "document_type": row.get("Document Type", ''),
        "eligible": row.get("Eligible", ''),
        "inclusion_date": row.get("Inclusion Date", ''),
        "uc_approval_date": row.get("UC Approval Date", ''),
        "article_access_type": row.get("Article Access Type", ''),
        "article_license": row.get("Article License", ''),
        "journal_name": row.get("Journal Name", ''),
        "issn_eissn": row.get("ISSN/eISSN", ''),
        "journal_access_type": row.get("Journal Access Type", ''),
        "journal_subject": row.get("Journal Subject", ''),
        "grant_participation": row.get("Grant Participation", ''),
        "funder_information": row.get("Funder Information", ''),
        "full_coverage_reason": row.get("Full Coverage Reason", ''),
        "original_apc_usd": row.get("Original APC (USD)", ''),
        "contractual_apc_usd": row.get("Contractual APC (USD)", ''),
        "library_apc_portion_usd": row.get("Library APC Portion (USD)", ''),
        "author_apc_portion_usd": row.get("Author APC Portion (USD)", ''),
        "payment_note": row.get("Payment Note", ''),
        "cdl_notes": row.get("CDL Notes", ''),
        "license_chosen": row.get("License Chosen", ''),
        "journal_bucket": row.get("Journal Bucket", ''),
        "agreement_manager_profile_name": row.get("Agreement Manager Profile Name", ''),
        "publisher_status": row.get("Publisher Status", ''),
        }
    return record

def main():

    publisher = None
    if (len(sys.argv) == 2):
        publisher = sys.argv[1]
    elif (len(sys.argv) != 1):
        usage()
        exit(1)

    print("Processing started: {}".format(datetime.now()))

    db_conn_str = get_db_conn_str()
    database = init_database(db_conn_str)

    if publisher:
        process_one_publisher(publisher, database)
    else:
        process_all_publishers(database)

    print("Processing finished: {}".format(datetime.now()))

if __name__ == "__main__":
    main()
