# Elsevier transformer
from utils import *

source_fieldnames = [
        "Corr. Author Name",
        "Corr. Author Email",
        "Journal ISSN",
        "Journal Title",
        "DOI",
        "Full Article Title",
        "Institute ECR",
        "Institute Name",
        "License Type",
        "PII",
        "Publication Item Type",
        "Article Access Type",
        "Journal Buckets",
        "Acceptance Date",
        "Author Journey Completion Date",
        "Grant Participation",
        "Research Funder",
        "Reason Author Chose Not to Pay the Remainder",
        "APC List Price",
        "Discount % on APC",
        "Discount on APC",
        "UC Subvention",
        "APC After Discount",
        "APC Remainder",
        "Author Portion of Remainder",
        "Library Portion of Remainder"
        ]


def source_to_output_mapping(row):
    output = {
        'Publisher': "Elsevier",
        'DOI': row['DOI'],
        'Article Title': row['Full Article Title'],
        'Corresponding Author': row['Corr. Author Name'],
        'Corresponding Author Email': row['Corr. Author Email'],
        'UC Institution': row['Institute Name'],
        'Institution Identifier': row['Institute ECR'],
        'Document Type': row['Publication Item Type'],
        'Eligible': "Yes",
        'Inclusion Date': row['Acceptance Date'],
        'UC Approval Date': "",
        'Article Access Type': row['Article Access Type'],
        'Article License': row['License Type'],
        'Journal Name': row['Journal Title'],
        'ISSN/eISSN': row['Journal ISSN'],
        'Journal Access Type': row['Journal Buckets'],
        'Journal Subject': "",
        'Grant Participation': row['Grant Participation'],
        'Funder Information': row['Research Funder'],
        'Full Coverage Reason': row['Reason Author Chose Not to Pay the Remainder'],
        'Original APC (USD)': row['APC List Price'],
        'Contractual APC (USD)': row['APC After Discount'],
        'Library APC Portion (USD)': str_to_decimal(row['UC Subvention']) + str_to_decimal(row['Library Portion of Remainder']),
        'Author APC Portion (USD)': row['Author Portion of Remainder'],
        'Payment Note': "",
        'CDL Notes': "",
        'Journal Bucket': row['Journal Buckets'],
        }
    return output

