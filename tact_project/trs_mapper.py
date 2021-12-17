# Royal Society transformer

source_fieldnames = [
        "id",
        "type",
        "state",
        "created",
        "e1",
        "e2",
        "p1",
        "p2",
        "from",
        "to",
        "corresponding author",
        "corresponding author affiliations",
        "authors",
        "affiliations",
        "journal",
        "journal id",
        "in DOAJ",
        "manuscript / article title",
        "article type",
        "article doi",
        "article doi url",
        "funders",
        "funder acknowlegdement",
        "grants",
        "manuscript id",
        "submission id",
        "manuscript submission date",
        "manuscript acceptance date",
        "article publication date",
        "preprint",
        "preprint url",
        "preprint id",
        "journal type",
        "VoR license",
        "VoR deposition",
        "VoR research data",
        ]


def source_to_output_mapping(row):
    output = {
        'Publisher': "Royal Society",
        'DOI': row['article doi'],
        'Article Title': row['manuscript / article title'],
        'Corresponding Author': row['corresponding author'],
        'Corresponding Author Email': "",
        'UC Institution': row['corresponding author affiliations'],
        'Institution Identifier': "",
        'Document Type': row['article type'],
        'Eligible': "Yes",
        'Inclusion Date': row['manuscript acceptance date'],
        'UC Approval Date': "",
        'Article Access Type': "OA",
        'Article License': row['VoR license'],
        'Journal Name': row['journal'],
        'ISSN/eISSN': row['journal id'],
        'Journal Access Type': row['journal type'],
        'Journal Subject': "",
        'Grant Participation': "",
        'Funder Information': row['funders'],
        'Full Coverage Reason': "",
        'Original APC (USD)': "",
        'Contractual APC (USD)': "",
        'Library APC Portion (USD)': "",
        'Author APC Portion (USD)': "",
        'Payment Note': "",
        'CDL Notes': "",

        }
    return output
