import os
import string

from datetime import datetime
from csv import DictReader
from csv import DictWriter

import acm_transformer

fieldnames_output = [
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
        "License Chosen"
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

def transform(publisher, input_filename, output_filename):
    output_file = open(output_filename, 'w', newline='', encoding='UTF-8')
    writer = DictWriter(output_file, fieldnames=fieldnames_output)
    writer.writeheader()

    with open(input_filename, 'r', newline='', encoding='UTF-8') as csvfile:
        if publisher == "ACM":
            fieldnames = acm_transformer.source_fieldnames
        reader = DictReader(csvfile, fieldnames=fieldnames)
        next(reader, None)  # skip the headers
        for row in reader:
            if publisher == "ACM":
                output_row = acm_transformer.source_to_output_mapping(row)
                transform_acm(output_row)
            writer.writerow(output_row)

    output_file.close()


def transform_acm(row):
    row['Article Title'] = normalized_article_title(row['Article Title'])
    row['UC Institution'] = get_institution_name(row['UC Institution'])
    row['Inclusion Date'] = normalized_date(row['Inclusion Date'], row['DOI'])
    row['Journal Access Type'] =  get_journal_access_type(row['Journal Name'])
    return row

def get_institution_name(name):
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

def get_journal_access_type(publication_title):
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

def test_remove_punctuation():
    title = " Thank you Human-Robot!  -- You're welcome. "
    converted = "Thank you Human Robot You're welcome"
    assert(converted == normalized_publication_title(title))

def main():
    input_filename = "./indata/ACM/ACM_UC_Report_Input.csv"
    output_filename = "./outputs/ACM/ACM_output.csv"
    transform("ACM", input_filename, output_filename)

if __name__ == "__main__":
    main()
