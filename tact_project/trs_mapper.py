# TRS - The Royal Society mapper 

def source_to_output_mapping(src_row, output_row):
    output_row['Publisher'] = "Royal Society"
    output_row['DOI'] = src_row['article doi']
    output_row['Article Title'] = src_row['manuscript / article title']
    output_row['Corresponding Author'] = src_row['corresponding author']
    output_row['UC Institution'] = src_row['corresponding author affiliations']
    output_row['Document Type'] = src_row['article type']
    output_row['Eligible'] = "Yes"
    output_row['Inclusion Date'] = src_row['manuscript acceptance date']
    output_row['Article Access Type'] = "OA"
    output_row['Article License'] = src_row['VoR license']
    output_row['Journal Name'] = src_row['journal']
    output_row['ISSN/eISSN'] = src_row['journal id']
    output_row['Journal Access Type'] = src_row['journal type']
    output_row['Funder Information'] = src_row['funders']

