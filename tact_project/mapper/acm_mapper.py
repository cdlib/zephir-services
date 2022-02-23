# ACM mapper

def source_to_output_mapping(src_row, output_row):
    output_row['Publisher'] = "ACM"
    output_row['DOI'] = src_row['DOI']
    output_row['Article Title'] = src_row['Title']
    output_row['Corresponding Author'] = src_row['First Name'] + " " + src_row['Last Name']
    output_row['Corresponding Author Email'] = src_row['Email']
    output_row['UC Institution'] = src_row['name']
    output_row['Eligible'] = "Yes"
    output_row['Inclusion Date'] = src_row['Date Published']
    output_row['Article Access Type'] = "OA"
    output_row['Article License'] = src_row['CC License']
    output_row['Journal Name'] = src_row['Publication Title']
    output_row['ISSN/eISSN'] = src_row['Publication ISSN/ISBN']
    output_row['Journal Access Type'] = src_row['Publication Title']
    output_row['Journal Subject'] = "STM"
    output_row['Payment Note'] = src_row['Notes']
    output_row['License Chosen'] = src_row['License Chosen']


