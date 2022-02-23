# COB mapper
from utils import str_to_decimal 

def source_to_output_mapping(src_row, output_row):
    output_row['Publisher'] = "CoB"
    output_row['DOI'] = src_row['Manuscript DOI']
    output_row['Article Title'] = src_row['Manuscript Name']
    output_row['Corresponding Author'] = "{} {} {}".format(src_row['Primary Author First Name'], src_row['Primary Author Middle Name'], src_row['Primary Author Last Name'])
    output_row['Corresponding Author Email'] = src_row['Primary Author Email Address']
    output_row['Document Type'] = src_row['Manuscript Type']
    output_row['Eligible'] = "Yes"
    output_row['Inclusion Date'] = src_row['Date Manuscript Accepted']
    output_row['Article Access Type'] = "OA"
    output_row['Article License'] = src_row['Creative Commons License Type']
    output_row['Journal Name'] = src_row['Publication Name']
    output_row['ISSN/eISSN'] = src_row['Publication ID']
    output_row['Grant Participation'] = src_row['Product 1 Option 2 Value']
    output_row['Funder Information'] = src_row['Funder 1 Name']
    output_row['Full Coverage Reason'] = src_row['Product 1 Option 3 Value']
    output_row['Library APC Portion (USD)'] = str_to_decimal(src_row['Total Discount'])

