# PLOS transformer
from utils import * 

source_fieldnames = [
        "Document Number",
        "Name",
        "Account",
        "Gross Amount",
        "Discount",
        "Paid by Institution",
        "PLOS Paid by Author",
        "Item",
        "Author Details",
        "First Name",
        "Last Name",
        "Institution",
        "Organization Department",
        "Billing Address",
        "Phone",
        "Email",
        "Manuscript Number",
        "Dtitle",
        "Issn",
        "DOI",
        "Product",
        "Original Submission Date",
        "Final Decision Date",
        "Funding Disclosure",
        "Memo",
        ]


def source_to_output_mapping(row):
    output = {
        'DOI': row['DOI'],
        'Article Title': row['Dtitle'],
        'Corresponding Author': row['First Name'] + " " + row['Last Name'],
        'Corresponding Author Email': row['Email'],
        'UC Institution': row['Institution'],
        'Institution Identifier': "",
        'Document Type': "",
        'Eligible': 'Yes',
        'Inclusion Date': row['Final Decision Date'],
        'UC Approval Date': "",
        'Article Access Type': "OA",
        'Article License': "",
        'Journal Name': row['Product'],
        'ISSN/eISSN': row['Issn'],
        'Journal Access Type': "Fully OA",
        'Journal Subject': "",
        'Grant Participation': row['PLOS Paid by Author'],
        'Funder Information': row['Funding Disclosure'],
        'Full Coverage Reason': "",
        'Original APC (USD)': row['Gross Amount'],
        'Contractual APC (USD)': str_to_decimal(row['Gross Amount']) - str_to_decimal(row['Discount']),
        'Library APC Portion (USD)': row['Paid by Institution'],
        'Author APC Portion (USD)': row['PLOS Paid by Author'],
        'Payment Note': "",
        'CDL Notes': "",
        }
    return output

