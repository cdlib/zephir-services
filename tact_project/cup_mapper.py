# CUP transformer
from utils import *

source_fieldnames = [
        "Institution",
        "Grid ID",
        "Manuscript Title",
        "DOI",
        "Corresponding Author",
        "Corresponding Author Email",
        "Core Article Type",
        "Copy Received Date",
        "Creative Commons License Type",
        "Open Access",
        "Eligible",
        "Notes",
        "Agreement Manager Profile Name",
        "Total Transaction Value",
        "Transaction value before discount",
        "Author Choice for Additional APC",
        "Reason Given for Requesting Full Funding",
        "Additional APC Paid by CDL",
        "Additional APC Paid by Author",
        "Publication Name",
        "Online ISSN",
        "HSS/STM",
        "Journal Status",
        "APC USD/AUD/EUR"
        ]


def source_to_output_mapping(src_row, output_row):
    output_row['Publisher'] = "CUP"
    output_row['DOI'] = src_row['DOI']
    output_row['Article Title'] = src_row['Manuscript Title']
    output_row['Corresponding Author'] = src_row['Corresponding Author']
    output_row['Corresponding Author Email'] = src_row['Corresponding Author Email']
    output_row['UC Institution'] = src_row['Institution']
    output_row['Institution Identifier'] = src_row['Grid ID']
    output_row['Document Type'] = src_row['Core Article Type']
    output_row['Eligible'] = src_row['Eligible?']
    output_row['Inclusion Date'] = src_row['Copy Received Date']
    output_row['Article Access Type'] = src_row['Open Access?']
    output_row['Article License'] = src_row['Creative Commons License Type']
    output_row['Journal Name'] = src_row['Publication Name']
    output_row['ISSN/eISSN'] = src_row['Online ISSN']
    output_row['Journal Access Type'] = src_row['Journal Status']
    output_row['Journal Subject'] = src_row['HSS/STM']
    output_row['Grant Participation'] = src_row['Author Choice for Additional APC']
    output_row['Full Coverage Reason'] = src_row['Reason Given for Requesting Full Funding']
    output_row['Original APC (USD)'] = str_to_decimal(src_row['APC USD/AUD/EUR'])
    output_row['Contractual APC (USD)'] = str_to_decimal(src_row['Total Transaction Value'])
    output_row['Library APC Portion (USD)'] = str_to_decimal(src_row['Transaction value before discount']) + str_to_decimal(src_row['Additional APC Paid by CDL'])
    output_row['Author APC Portion (USD)'] = str_to_decimal(src_row['Additional APC Paid by Author'])
    output_row['Payment Note'] = src_row['Notes']
    output_row['Agreement Manager Profile Name'] = src_row['Agreement Manager Profile Name']
    output_row['Publisher Status'] = src_row['Status']

