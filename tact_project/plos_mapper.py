# PLOS mapper
from utils import str_to_decimal 

def source_to_output_mapping(src_row, output_row):
    output_row['Publisher'] = "PLOS"
    output_row['DOI'] = src_row['DOI']
    output_row['Article Title'] = src_row['Dtitle']
    output_row['Corresponding Author'] = src_row['First Name'] + " " + src_row['Last Name']
    output_row['Corresponding Author Email'] = src_row['Email']
    output_row['UC Institution'] = src_row['Institution']
    output_row['Eligible'] = "Yes"
    output_row['Inclusion Date'] = src_row['Final Decision Date']
    output_row['Article Access Type'] = "OA"
    output_row['Journal Name'] = src_row['Product']
    output_row['ISSN/eISSN'] = src_row['Issn']
    output_row['Journal Access Type'] = "Fully OA"
    output_row['Grant Participation'] = src_row['PLOS Paid by Author']
    output_row['Funder Information'] = src_row['Funding Disclosure']
    output_row['Original APC (USD)'] = str_to_decimal(src_row['Gross Amount'])
    output_row['Contractual APC (USD)'] = str_to_decimal(src_row['Gross Amount']) - str_to_decimal(src_row['Discount'])
    output_row['Library APC Portion (USD)'] = str_to_decimal(src_row['Paid by Institution'])
    output_row['Author APC Portion (USD)'] = str_to_decimal(src_row['PLOS Paid by Author'])

