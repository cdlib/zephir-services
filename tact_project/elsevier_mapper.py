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


def source_to_output_mapping(src_row, output_row):
    output_row['Publisher'] = "Elsevier"
    output_row['DOI'] = src_row['DOI']
    output_row['Article Title'] = src_row['Full Article Title']
    output_row['Corresponding Author'] = src_row['Corr. Author Name']
    output_row['Corresponding Author Email'] = src_row['Corr. Author Email']
    output_row['UC Institution'] = src_row['Institute Name']
    output_row['Institution Identifier'] = src_row['Institute ECR']
    output_row['Document Type'] = src_row['Publication Item Type']
    output_row['Eligible'] = "Yes"
    output_row['Inclusion Date'] = src_row['Acceptance Date']
    output_row['Article Access Type'] = src_row['Article Access Type']
    output_row['Article License'] = src_row['License Type']
    output_row['Journal Name'] = src_row['Journal Title']
    output_row['ISSN/eISSN'] = src_row['Journal ISSN']
    output_row['Journal Access Type'] = src_row['Journal Buckets']
    output_row['Grant Participation'] = src_row['Grant Participation']
    output_row['Funder Information'] = src_row['Research Funder']
    output_row['Full Coverage Reason'] = src_row['Reason Author Chose Not to Pay the Remainder']
    output_row['Original APC (USD)'] = str_to_decimal(src_row['APC List Price'])
    output_row['Contractual APC (USD)'] = str_to_decimal(src_row['APC After Discount'])
    output_row['Library APC Portion (USD)'] = str_to_decimal(src_row['UC Subvention']) + str_to_decimal(src_row['Library Portion of Remainder'])
    output_row['Author APC Portion (USD)'] = str_to_decimal(src_row['Author Portion of Remainder'])
    output_row['Journal Bucket'] = src_row['Journal Buckets']

