# ACM transformer

source_fieldnames = [
        "DOI",
        "Title",
        "License Chosen",
        "CC License",
        "Date Published",
        "name",
        "Publication Title",
        "Publication ISSN/ISBN",
        "First Name",
        "Last Name",
        "Email",
        "Affiliation",
        "Notes"
        ]


def source_to_output_mapping(row):
    output = {
        'Publisher': "ACM",
        'DOI': row['DOI'],
        'Article Title': row['Title'],
        'Corresponding Author': row['First Name'] + " " + row['Last Name'],
        'Corresponding Author Email': row['Email'],
        'UC Institution': row['name'],
        'Institution Identifier': "",
        'Document Type': "",
        'Eligible': "Yes",
        'Inclusion Date': row['Date Published'],
        'UC Approval Date': "",
        'Article Access Type': "OA",
        'Article License': row['CC License'],
        'Journal Name': row['Publication Title'],
        'ISSN/eISSN': row['Publication ISSN/ISBN'],
        'Journal Access Type': row['Publication Title'],
        'Journal Subject': "STM",
        'Grant Participation': "",
        'Funder Information': "",
        'Full Coverage Reason': "",
        'Original APC (USD)': 0,
        'Contractual APC (USD)': 0,
        'Library APC Portion (USD)': 0,
        'Author APC Portion (USD)': 0,
        'Payment Note': row['Notes'],
        'CDL Notes': "",
        'License Chosen': row['License Chosen'],
        }
    return output

