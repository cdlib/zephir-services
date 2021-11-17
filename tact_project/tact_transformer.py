import os
import sys

import string
import re
from datetime import datetime
from csv import DictReader
from csv import DictWriter
from pathlib import Path
from pathlib import PurePosixPath

from utils import *

import acm_transformer
import elsevier_transformer
import springer_transformer
import cup_transformer
import plos_transformer
import trs_transformer

publishers = [
        "ACM",
        "CoB",
        "CSP",
        "CUP",
        "Elsevier",
        "JMIR",
        "PLOS",
        "PNAS"
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
        "Disease Models & Mechanisms",
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
        "PACMPL"
        ]

def define_variables(publisher):
    publisher = publisher.lower()
    if publisher == "acm":
        source_fieldnames = acm_transformer.source_fieldnames
        mapping_function = getattr(acm_transformer, "source_to_output_mapping")
        transform_function = globals()['transform_acm']
    elif publisher == "cup":
        source_fieldnames = cup_transformer.source_fieldnames
        mapping_function = getattr(cup_transformer, "source_to_output_mapping")
        transform_function = globals()['transform_cup']
    elif publisher == "elsevier":
        source_fieldnames = elsevier_transformer.source_fieldnames
        mapping_function = getattr(elsevier_transformer, "source_to_output_mapping")
        transform_function = globals()['transform_elsevier']
    elif publisher == "plos":
        source_fieldnames = plos_transformer.source_fieldnames
        mapping_function = getattr(plos_transformer, "source_to_output_mapping")
        transform_function = globals()['transform_plos']
    elif publisher == "springer":
        source_fieldnames = springer_transformer.source_fieldnames
        mapping_function = getattr(springer_transformer, "source_to_output_mapping")
        transform_function = globals()['transform_springer']
    elif publisher == "trs":
        source_fieldnames = trs_transformer.source_fieldnames
        mapping_function = getattr(trs_transformer, "source_to_output_mapping")
        transform_function = globals()['transform_trs']

    return source_fieldnames, mapping_function, transform_function

def transform(publisher, input_filename, output_filename):

    source_fieldnames, mapping_function, transform_function = define_variables(publisher)

    output_file = open(output_filename, 'w', newline='', encoding='UTF-8')
    writer = DictWriter(output_file, fieldnames=output_fieldnames)
    writer.writeheader()

    with open(input_filename, 'r', newline='', encoding='UTF-8') as csvfile:
        reader = DictReader(csvfile, fieldnames=source_fieldnames)
        next(reader, None)  # skip the headers
        for row in reader:
            output_row = mapping_function(row)
            if output_row['DOI'].strip():
                transform_function(output_row)
                writer.writerow(output_row)

    output_file.close()


def transform_acm(row):
    row['Article Title'] = normalized_article_title(row['Article Title'])
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Journal Access Type'] =  normalized_journal_access_type_by_title(row['Journal Name'])
    return row

def transform_cup(row):
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Article Access Type'] = normalized_article_access_type(row['Article Access Type'])
    row['Journal Access Type'] = normalized_journal_access_type(row['Journal Access Type'])
    row['Grant Participation'] = normalized_grant_participation_cup(row['Grant Participation'])
    if "I have research funds available to pay the remaining balance due (you will be asked to pay the Additional Charge on a later screen)" in row['Full Coverage Reason']:
        row['Full Coverage Reason'] = ""

    return row

def transform_elsevier(row):
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Article Access Type'] = normalized_article_access_type(row['Article Access Type'])
    row['Journal Access Type'] = normalized_journal_access_type(row['Journal Access Type'])
    row['Grant Participation'] = normalized_grant_participation(row['Grant Participation'])
    return row

def transform_plos(row):
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Journal Name'] = normalized_journal_name_plos(row['Journal Name'])
    row['Grant Participation'] = "Yes" if str_to_decimal(row['Grant Participation']) > 0 else "No"
    return row

def transform_springer(row):
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Eligible'] = "Yes" if row['Eligible'].lower() in ["approved", "opt-out"] else "No"
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['UC Approval Date'] = normalized_date(row['UC Approval Date'], row['DOI'])
    row['Article Access Type'] = normalized_article_access_type(row['Article Access Type'])
    row['Grant Participation'] = normalized_grant_participation(row['Grant Participation'])
    return row

def transform_trs(row):
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Journal Access Type'] = normalized_journal_access_type(row['Journal Access Type'])

def normalized_institution_name(name):
    """Institution Look-up:
    University of California => UC System
    University of California System => "UC System"

    University of California Berkeley School of Public Health => UC Berkeley

    University of California Division of Agriculture and Natural Resources => UC Davis
    USDA Agricultural Research Service => UC Davis
    Univeristy of California, Davis School of Veterinary Medicine => UC Davis
    University of California Davis School of Medicine => UC Davis

    University of California Irvine Medical Center => UC Irvine

    Containing UCLA => UC Los Angeles

    University of California,Institute for Integrative Genome Biology => UC Riverside

    Department of Psychological and Brain Sciences,University of California => UC Santa Barbara
    National Center for Ecological Analysis and Synthesis => UC Santa Barbara

    University of California - San Diego School of Medicine => UC San Diego

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
    """
    name = name.strip()
    if name == "University of California" or name =="University of California System":
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
    return ("University of California" in name or "UC" in name) and keyword in name

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
    if "Hybrid".lower() in journal_access_type.lower():
        return "Hybrid"
    elif "Gold" in journal_access_type or "pure OA" in journal_access_type: 
        return "Fully OA"
    elif journal_access_type == "No OA":
        return "Subscription"
    else:
        return ""

def normalized_article_access_type(article_access_type):
    article_access_type = article_access_type.capitalize()
    if article_access_type in ["Hybrid Open Access", "Full Open Access", "Approved", "Yes"]:
        return "OA"
    elif article_access_type in ["Subscription", "Opt-Out", "No"]:
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

def normalized_date(date_str, doi):
    # 1/31/21 => 01/31/2021
    # 01/31/2021: keep as is
    # 2021-06-24 21:18:29 => 06/24/2021
    normalized_date = ''
    if date_str:
        date_str = date_str.strip()
        try:
            normalized_date = datetime.strptime(date_str , '%m/%d/%y').strftime('%m/%d/%Y')
        except ValueError:
            try:
                normalized_date = datetime.strptime(date_str, '%m/%d/%Y').strftime('%m/%d/%Y')
            except ValueError:
                try:
                    normalized_date = datetime.strptime(date_str[0:10] , '%Y-%m-%d').strftime('%m/%d/%Y')
                except ValueError:
                    print("Date format error: {} - {} ".format(date_str, doi))
    
    return normalized_date

def normalized_grant_participation(grant_participation):
    if grant_participation in ["Y", "Yes", "Partially Covered"]:
        return "Yes"
    elif grant_participation in ["N", "No", "Fully Covered"]:
        return "No"
    return ""

def normalized_grant_participation_cup(grant_participation):
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

def test_remove_punctuation():
    title = " Thank you Human-Robot!  -- You're welcome. "
    converted = "Thank you Human Robot You're welcome"
    assert(converted == normalized_publication_title(title))

def process_one_publisher(publisher):
    publisher = publisher.strip().lower()

    input_dir = Path(os.getcwd()).joinpath("./indata/{}".format(publisher))
    output_dir = Path(os.getcwd()).joinpath("./outputs/{}".format(publisher))
    processed_dir = Path(os.getcwd()).joinpath("./processed/{}".format(publisher))
    input_files = (entry for entry in input_dir.iterdir() if entry.is_file())
    for input_file in input_files:
        file_extension = PurePosixPath(input_file).suffix
        filename_wo_ext = PurePosixPath(input_file).stem
        if file_extension == ".csv":
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S_%f')
            output_filename = output_dir.joinpath("{}_output_{}.csv".format(filename_wo_ext, timestamp))
            transform(publisher, input_file, output_filename)

            input_file.rename(processed_dir.joinpath(input_file.name))


def process_all_publishers():
    for publisher in publishers:
        process_one_publisher(publisher)

def usage():
    print("Parameter error.")
    print("Usage: {} publisher_name(optional)".format(sys.argv[0]))
    print("For example: {} Elsevier".format(sys.argv[0]))
    print("For example: {}".format(sys.argv[0]))
    print("Processing files from specified publisher when publisher name is provided")
    print("Otherwise processing files from all publishers")
    print("Publisher name is case insensitive")

def main():

    publisher = None
    if (len(sys.argv) == 2):
        publisher = sys.argv[1]
    elif (len(sys.argv) != 1):
        usage()
        exit(1)

    if publisher:
        process_one_publisher(publisher)
    else:
        process_all_publishers()

if __name__ == "__main__":
    main()
