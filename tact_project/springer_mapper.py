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


def source_to_output_mapping(src_row, output_row):
    output_row['Publisher'] = "Springer"
    output_row['DOI'] = src_row['DOI']
    output_row['Article Title'] = src_row['Article Title']
    output_row['Corresponding Author'] = src_row['Corresponding Author Name']
    output_row['UC Institution'] = src_row['Corresponding Author Affiliation']
    output_row['Document Type'] = src_row['Article Type']
    output_row['Eligible'] = src_row['Approval Status']
    output_row['Inclusion Date'] = src_row['Approval Date']
    output_row['UC Approval Date'] = src_row['Approval Date']
    output_row['Article Access Type'] = src_row['Approval Status']
    output_row['Article License'] = src_row['OA License Type']
    output_row['Journal Name'] = src_row['Journal Title']
    output_row['ISSN/eISSN'] = src_row['Journal ISSN']
    output_row['Journal Access Type'] = src_row['Agreement']
    output_row['Grant Participation'] = src_row['Fee Model']
    output_row['Funder Information'] = src_row['Funder Name']
    output_row['Full Coverage Reason'] = src_row['Full Coverage Reason']
    output_row['Original APC (USD)'] = str_to_decimal(src_row['List APC (USD)'])
    output_row['Contractual APC (USD)'] = str_to_decimal(src_row['Licensee APC (USD)'])
    output_row['Library APC Portion (USD)'] = str_to_decimal(src_row['Insitutional APC Share (USD)'])
    output_row['Author APC Portion (USD)'] = str_to_decimal(src_row['Author APC Share (USD)'])
    output_row['Payment Note'] = src_row['Comments']

