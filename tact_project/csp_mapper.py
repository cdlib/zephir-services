# CSP mapper 
from utils import *

source_fieldnames = [
                "DOI",
        "Title",
        "Corresponding Author",
        "Corresponding Author Email",
        "Corresponding Author Institution/Affiliation",
        "Institution Identifier",
        "Document Type",
        "Eligible",
        "Inclusion Date",
        "UC Approval Date",
        "Article Open Access Status",
        "License",
        "Journal Name",
        "ISSN/eISSN",
        "Journal Access Type",
        "Subject",
        "Grant Participation",
        "Funder Information",
        "Full Coverage Reason",
        "Original APC (USD)",
        "Contractual APC (USD)",
        "Library APC Portion (USD)",
        "Author APC Portion (USD)",
        "Payment Note",
        "Status",
        ]


def source_to_output_mapping(row):
    output = {
        'Publisher': "CSP",
        'DOI': row['DOI'],
        'Article Title': row['Title'],
        'Corresponding Author': row['Corresponding Author'],
        'Corresponding Author Email': row['Corresponding Author Email'],
        'UC Institution': row['Corresponding Author Institution/Affiliation'],
        'Institution Identifier': row['Institution Identifier'],
        'Document Type': row['Document Type'], 
        'Eligible': row['Eligible'],
        'Inclusion Date': row['Inclusion Date'], 
        'UC Approval Date': row['UC Approval Date'] if row['UC Approval Date'] != "N/A" else "", 
        'Article Access Type': row['Article Open Access Status'],
        'Article License': row['License'],
        'Journal Name': row['Journal Name'], 
        'ISSN/eISSN': row['ISSN/eISSN'],
        'Journal Access Type': row['Journal Access Type'], 
        'Journal Subject': row['Subject'],
        'Grant Participation': row['Grant Participation'],
        'Funder Information': row['Funder Information'],
        'Full Coverage Reason': row['Full Coverage Reason'] if row['Full Coverage Reason'].upper() != "N/A" else "",
        'Original APC (USD)': str_to_decimal(row['Original APC (USD)']),
        'Contractual APC (USD)': str_to_decimal(row['Contractual APC (USD)']),
        'Library APC Portion (USD)': str_to_decimal(row['Library APC Portion (USD)']),
        'Author APC Portion (USD)': str_to_decimal(row['Author APC Portion (USD)']),
        'Payment Note': row['Payment Note'], 
        'Publisher Status': row['Status'],
        'CDL Notes': '',

        }
    return output

