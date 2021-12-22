# Springer transformer
from utils import *

source_fieldnames = [
        "Agreement",
        "UAI",
        "DOI",
        "Journal ID",
        "Journal ISSN",
        "Journal Title",
        "Article Title",
        "Article Type",
        "OA License Type",
        "Corresponding Author Name",
        "Corresponding Author Affiliation",
        "Corresponding Author ORCID ID",
        "Accepted Date",
        "Approval Status",
        "Approval Date",
        "Approving Institution",
        "Online Publication Date",
        "Fee Model",
        "Full Coverage Reason",
        "List APC (USD)",
        "Licensee APC (USD)",
        "Insitutional APC Share (USD)",
        "Author APC Share (USD)",
        "Funder Name",
        "Comments"
        ]


def source_to_output_mapping(row):
    output = {
        'Publisher': "Springer",
        'DOI': row['DOI'],
        'Article Title': row['Article Title'],
        'Corresponding Author': row['Corresponding Author Name'],
        'Corresponding Author Email': "",
        'UC Institution': row['Corresponding Author Affiliation'],
        'Institution Identifier': "",
        'Document Type': row['Article Type'],
        'Eligible': row['Approval Status'],
        'Inclusion Date': row['Approval Date'],
        'UC Approval Date': row['Approval Date'],
        'Article Access Type': row['Approval Status'],
        'Article License': row['OA License Type'],
        'Journal Name': row['Journal Title'],
        'ISSN/eISSN': row['Journal ISSN'],
        'Journal Access Type': row['Agreement'],
        'Journal Subject': "",
        'Grant Participation': row['Fee Model'],
        'Funder Information': row['Funder Name'],
        'Full Coverage Reason': row['Full Coverage Reason'],
        'Original APC (USD)': str_to_decimal(row['List APC (USD)']),
        'Contractual APC (USD)': str_to_decimal(row['Licensee APC (USD)']),
        'Library APC Portion (USD)': str_to_decimal(row['Insitutional APC Share (USD)']),
        'Author APC Portion (USD)': str_to_decimal(row['Author APC Share (USD)']),
        'Payment Note': row['Comments'],
        'CDL Notes': "",
        }
    return output

