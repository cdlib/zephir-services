import os
import sys

import string
from datetime import datetime
from csv import DictReader
from csv import DictWriter
from pathlib import Path
from pathlib import PurePosixPath

import acm_transformer
import elsevier_transformer

publishers = [
        "ACM",
        "CoB",
        "CSP",
        "CUP",
        "Elsevier",
        "JMIR",
        "RoyalS",
        "Springer",
        "PNAS"
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
        "Journal Bucket"
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
    elif publisher == "elsevier":
        source_fieldnames = elsevier_transformer.source_fieldnames
        mapping_function = getattr(elsevier_transformer, "source_to_output_mapping")
        transform_function = globals()['transform_elsevier']

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
            transform_function(output_row)

            writer.writerow(output_row)

    output_file.close()


def transform_acm(row):
    row['Article Title'] = normalized_article_title(row['Article Title'])
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Journal Access Type'] =  normalized_gournal_access_type_by_title(row['Journal Name'])
    return row

def transform_elsevier(row):
    row['UC Institution'] = normalized_institution_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Article Access Type'] = normalized_article_access_type(row['Article Access Type'])
    row['Journal Access Type'] = normalized_gournal_access_type(row['Journal Access Type'])
    row['Grant Participation'] = normalized_grant_participation(row['Grant Participation'])
    return row

def normalized_institution_name(name):
    """Institution Look-up:
    University of California, Davis => "UC Davis"
    University of California Davis => "UC Davis"
    Lawrence Berkeley National Laboratory => LBNL
    Lawrence Livermore National Laboratory => LLNL
    """

    if name == "Lawrence Berkeley National Laboratory":
       return "LBNL"
    elif name == "Lawrence Livermore National Laboratory":
        return "LLNL"
    else:
        return name.replace("University of California,", "UC").replace("University of California", "UC")

    return name

def normalized_gournal_access_type_by_title(publication_title):
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

def normalized_gournal_access_type(journal_access_type):
    """If string contains Hybrid, then Hybrid; If string contains Fully Gold, then Fully OA
    """
    if "Hybrid" in journal_access_type:
        return "Hybrid"
    elif "Fully Gold" in journal_access_type:
        return "Fully OA"
    else:
        return ""

def normalized_article_access_type(article_access_type):
    if article_access_type == "Hybrid Open Access":
        return "OA"
    elif article_access_type == "Full Open Access":
        return "OA"
    elif article_access_type == "Subscription":
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

def normalized_date(date, doi):
    # 1/31/21 => 01/31/2021
    # 01/31/2021: keep as is
    normalized_date = ''
    if date.strip():
        try:
            normalized_date = datetime.strptime(date.strip() , '%m/%d/%y').strftime('%m/%d/%Y')
        except ValueError:
            try:
                normalized_date = datetime.strptime(date.strip() , '%m/%d/%Y').strftime('%m/%d/%Y')
            except ValueError:
                print("Date format error: {} - {} ".format(date, doi))
    
    return normalized_date

def normalized_grant_participation(grant_participation):
    if grant_participation == 'Y':
        return "Yes"
    elif grant_participation == 'N':
        return "No"
    return ""

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
            print(input_file)
            print(input_file.name)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S_%f')
            output_filename = output_dir.joinpath("{}_output_{}.csv".format(filename_wo_ext, timestamp))
            transform(publisher, input_file, output_filename)

            input_file.rename(processed_dir.joinpath(input_file.name))


def process_all_publishers():
    for publisher in publishers:
        print(publisher)
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
