# CSP mapper 
from lib.utils import str_to_decimal 

def source_to_output_mapping(src_row, output_row):
    output_row['Publisher'] = "CSP"
    output_row['DOI'] = src_row['DOI']
    output_row['Article Title'] = src_row['Title']
    output_row['Corresponding Author'] = src_row['Corresponding Author']
    output_row['Corresponding Author Email'] = src_row['Corresponding Author Email']
    output_row['UC Institution'] = src_row['Corresponding Author Institution/Affiliation']
    output_row['Institution Identifier'] = src_row['Institution Identifier']
    output_row['Document Type'] = src_row['Document Type'] 
    output_row['Eligible'] = src_row['Eligible']
    output_row['Inclusion Date'] = src_row['Inclusion Date'] 
    output_row['UC Approval Date'] = src_row['UC Approval Date'] if src_row['UC Approval Date'].upper() != "N/A" else ""
    output_row['Article Access Type'] = src_row['Article Open Access Status']
    output_row['Article License'] = src_row['License']
    output_row['Journal Name'] = src_row['Journal Name'] 
    output_row['ISSN/eISSN'] = src_row['ISSN/eISSN']
    output_row['Journal Access Type'] = src_row['Journal Access Type'] 
    output_row['Journal Subject'] = src_row['Subject']
    output_row['Grant Participation'] = src_row['Grant Participation']
    output_row['Funder Information'] = src_row['Funder Information']
    output_row['Full Coverage Reason'] = src_row['Full Coverage Reason'] if src_row['Full Coverage Reason'].upper() != "N/A" else ""
    output_row['Original APC (USD)'] = str_to_decimal(src_row['Original APC (USD)'])
    output_row['Contractual APC (USD)'] = str_to_decimal(src_row['Contractual APC (USD)'])
    output_row['Library APC Portion (USD)'] = str_to_decimal(src_row['Library APC Portion (USD)'])
    output_row['Author APC Portion (USD)'] = str_to_decimal(src_row['Author APC Portion (USD)'])
    output_row['Payment Note'] = src_row['Payment Note'] 
    output_row['Publisher Status'] = src_row['Status']


