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


def source_to_output_mapping(row):
    output = {
        'Publisher': "CUP",
        'DOI': row['DOI'],
        'Article Title': row['Manuscript Title'],
        'Corresponding Author': row['Corresponding Author'],
        'Corresponding Author Email': row['Corresponding Author Email'],
        'UC Institution': row['Institution'],
        'Institution Identifier': row['Grid ID'],
        'Document Type': row['Core Article Type'],
        'Eligible': row['Eligible'],
        'Inclusion Date': row['Copy Received Date'],
        'UC Approval Date': '',
        'Article Access Type': row['Open Access'],
        'Article License': row['Creative Commons License Type'],
        'Journal Name': row['Publication Name'],
        'ISSN/eISSN': row['Online ISSN'],
        'Journal Access Type': row['Journal Status'],
        'Journal Subject': row['HSS/STM'],
        'Grant Participation': row['Author Choice for Additional APC'],
        'Funder Information': '',
        'Full Coverage Reason': row['Reason Given for Requesting Full Funding'],
        'Original APC (USD)': str_to_decimal(row['APC USD/AUD/EUR']),
        'Contractual APC (USD)': row['Total Transaction Value'],
        'Library APC Portion (USD)': str_to_decimal(row['Transaction value before discount']) + str_to_decimal(row['Additional APC Paid by CDL']),
        'Author APC Portion (USD)': row['Additional APC Paid by Author'],
        'Payment Note': row['Notes'],
        'CDL Notes': '',
        'Agreement Manager Profile Name': row['Agreement Manager Profile Name'],
        'Publisher Status': '',
        }
    return output

