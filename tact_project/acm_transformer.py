# ACM transformer

fieldnames = [
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


def transform(row):
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
        'Original APC (USD)': "",
        'Contractual APC (USD)': "",
        'Library APC Portion (USD)': "",
        'Author APC Portion (USD)': "",
        'Payment Note': row['Notes'],
        'CDL Notes': "",
        'License Chosen': row['License Chosen'],
        }
    return output

