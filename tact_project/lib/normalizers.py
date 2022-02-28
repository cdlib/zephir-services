from datetime import datetime
import string
import re

OPEN_ACCESS_PUBLICATION_TITLES = [
        "Disease Models Mechanisms",
        "Biology Open",
        "ACM Transactions on Architecture and Code Optimization",
        "ACM Transactions on Human Robot Interaction",
        "ACM/IMS Transactions on Data Science",
        "DGOV Research and Practice",
        "Digital Government Research and Practice",
        "Digital Threats Research and Practice",
        "PACM on Programming Languages",
        "Proceedings of the ACM on Programming Languages",
        "Transactions on Architecture and Code Optimization",
        "Transactions on Data Science",
        "Transactions on Human Robot Interaction",
        "TACO",
        "THRI",
        "TDS",
        "DGOV",
        "DTRAP",
        "PACMPL",
        ]

INSTITUTION_ID = {
        "UC Santa Cruz": "8787",
        "UC San Francisco": "8785",
        "UC Davis": "8789",
        "UC San Diego": "8784",
        "UC Berkeley": "1438",
        }

def normalized_date(date_str, doi=''):
    # 1/31/21 => 01/31/2021
    # 1/4/21 20:00 => 01/31/2021
    # 01/31/2021: keep as is
    # 2021-06-24 21:18:29 => 06/24/2021
    # 10-Jun-2021 - 10.1242/jeb.237628 => 06/10/2021
    # Jan 25, 2021 - cjfas-2020-0398.R1 => 01/25/2021
    normalized_date = ''
    if date_str:
        date_str = date_str.strip()
        date_1 = date_str.split()[0]
        try:
            normalized_date = datetime.strptime(date_1 , '%m/%d/%y').strftime('%m/%d/%Y')
        except ValueError:
            try:
                normalized_date = datetime.strptime(date_1, '%m/%d/%Y').strftime('%m/%d/%Y')
            except ValueError:
                try:
                    normalized_date = datetime.strptime(date_1 , '%Y-%m-%d').strftime('%m/%d/%Y')
                except ValueError:
                    try:
                        normalized_date = datetime.strptime(date_1 , '%d-%b-%Y').strftime('%m/%d/%Y')
                    except ValueError:
                        try:
                            date_1 = date_str.split('-')[0].strip()
                            normalized_date = datetime.strptime(date_1 , '%b %d, %Y').strftime('%m/%d/%Y')
                        except ValueError:
                            print("Date format error: {} - {} ".format(date_str, doi))

    return normalized_date


def normalized_institution_name(name):
    """Institution Look-up:
    University of California => UC System
    University of California System => "UC System"

    University of California Division of Agriculture and Natural Resources => UC Davis
    USDA Agricultural Research Service => UC Davis

    Containing UCLA => UC Los Angeles

    University of California,Institute for Integrative Genome Biology => UC Riverside

    Department of Psychological and Brain Sciences,University of California => UC Santa Barbara
    National Center for Ecological Analysis and Synthesis => UC Santa Barbara

    Containing UCSF => UC San Francisco
    Zuckerberg San Francisco General Hospital and Trauma Center => UC San Francisco
    Gladstone Institutes => UC San Francisco
    Chao Family Comprehensive Cancer Center => UC San Francisco
    
    Lawrence Berkeley National Laboratory => LBNL
    E O Lawrence Berkeley National Laboratory => LBNL
    Lawrence Livermore National Laboratory => LLNL

    General patterns:
    University of California, Davis => "UC Davis"
    University of California - Davis => "UC Davis"
    University of California Davis => "UC Davis"
    Univeristy of California, Davis School of Veterinary Medicine => UC Davis
    University of California Davis School of Medicine => UC Davis
    University of California Berkeley School of Public Health => UC Berkeley
    University of California Irvine Medical Center => UC Irvine
    University of California - San Diego School of Medicine => UC San Diego
    """
    name = name.strip()
    if name.lower() == "University of California".lower() or name.lower() == "University of California System".lower():
        return "UC System"
    elif school_name_matches(name, "Berkeley"):
        return "UC Berkeley"
    elif school_name_matches(name, "Davis") or "Division of Agriculture and Natural Resources" in name or "USDA Agricultural Research Service" in name:
        return "UC Davis"
    elif school_name_matches(name,"Irvine"):
        return "UC Irvine"
    elif school_name_matches(name,"Los Angeles") or "UCLA" in name:
        return "UC Los Angeles"
    elif school_name_matches(name,"Merced"):
        return "UC Merced"
    elif school_name_matches(name,"Riverside") or "Institute for Integrative Genome Biology" in name:
        return "UC Riverside"
    elif school_name_matches(name,"Santa Barbara") or "Department of Psychological and Brain Sciences" in name or "National Center for Ecological Analysis and Synthesis" in name:
        return "UC Santa Barbara"
    elif school_name_matches(name,"Santa Cruz"):
        return "UC Santa Cruz"
    elif school_name_matches(name,"San Diego"):
        return "UC San Diego"
    elif school_name_matches(name,"San Francisco") or "UCSF" in name or name in ["Zuckerberg San Francisco General Hospital and Trauma Center", 
            "Gladstone Institutes", 
            "Chao Family Comprehensive Cancer Center"]:
        return "UC San Francisco"
    elif "Lawrence Berkeley National Laboratory" in name:
       return "LBNL"
    elif "Lawrence Livermore National Laboratory" in name:
        return "LLNL"
    else:
        return name


def school_name_matches(name, keyword):
    return ("University of California" in name or "UC " in name) and keyword in name

def normalized_journal_access_type_by_title(publication_title):
    """Open Access look-up based on publication title.
    Normalize publication_title to change punctuation to space, change multiple spaces to single space before match. 
    Returns:
        "Fully OA": when publication_title is listed in OPEN_ACCESS_PUBLICATION_TITLES.
        "Hybrid": other cases.

    """
    if normalized_publication_title(publication_title) in OPEN_ACCESS_PUBLICATION_TITLES:
        return "Fully OA"
    else:
        return "Hybrid"

def normalized_journal_access_type(journal_access_type):
    journal_access_type = journal_access_type.lower()
    if "hybrid" in journal_access_type:
        return "Hybrid"
    elif "gold" in journal_access_type or "pure oa" in journal_access_type: 
        return "Fully OA"
    elif journal_access_type == "no oa":
        return "Subscription"
    else:
        return ""

def normalized_article_access_type(article_access_type):
    article_access_type = article_access_type.lower()
    if article_access_type in ["hybrid open access", "full open access", "approved", "yes"]:
        return "OA"
    elif article_access_type in ["subscription", "opt-out", "no"]:
        return "Subscription"
    else:
        return ""


def normalized_publication_title(title):
    title = title.replace('-', ' ')
    normalized_title = ' '.join(word.strip(string.punctuation) for word in title.split())
    normalized_title = ' '.join(normalized_title.split())  # replace multiple spaces with single space
    return normalized_title

def normalized_article_title(title):
    # change any "\"" to ""; change any "&#34;" to ""
    normalized_title = title.replace('\\"', '').replace('&#34;', '')
    return normalized_title

def normalized_grant_participation(grant_participation):
    if grant_participation in ["Y", "Yes", "Partially Covered"] or "I have research funds" in grant_participation:
        return "Yes"
    elif grant_participation in ["N", "No", "Fully Covered"] or "I do not have research funds" in grant_participation:
        return "No"
    return ""

#def normalized_grant_participation_2(grant_participation):
#    if "I have research funds" in grant_participation:
#        return "Yes"
#    elif "I do not have research funds" in grant_participation:
#        return "No"
#    return ""

def normalized_journal_name_plos(journal_name):
    journal_name = journal_name.strip()
    # drop leading three digit numerics + space "ddd "
    if re.search(r'^\d{3} ', journal_name):
        return journal_name[4:]
    return journal_name

def get_institution_by_email(email):
    email = email.lower()
    if "ucsc.edu" in email:
        return "UC Santa Cruz"
    elif "ucsf.edu" in email:
        return "UC San Francisco"
    elif "ucdavis.edu" in email:
        return "UC Davis"
    elif "ucd.edu" in email:
        return "UC Davis"
    elif "ucsd.edu" in email:
        return "UC San Diego"
    elif "berkeley.edu" in email:
        return "UC Berkeley"
    elif "uci.edu" in email:
        return "UC Irvine"
    elif "ucr.edu" in email:
        return "UC Riverside"
    elif "ucb.edu" in email:
        return "UC Berkeley"
    elif "ucla.edu" in email:
        return "UC Los Angeles"
    elif "ucm.edu" in email:
        return "UC Merced"
    elif "ucsb.edu" in email:
        return "UC Santa Barbara"
    else:
        return ""

def get_institution_id_by_name(name):
    try:
        return INSTITUTION_ID[name]
    except:
        return ""


def test_remove_punctuation():
    title = " Thank you Human-Robot!  -- You're welcome. "
    converted = "Thank you Human Robot You're welcome"
    assert(converted == normalized_publication_title(title))

def test_normalized_date():
    # 1/31/21 => 01/31/2021
    # 1/4/21 20:00 => 01/04/2021
    # 01/21/2021: keep as is
    # 2021-06-24 21:18:29 => 06/24/2021
    # 10-Jun-2021 - 10.1242/jeb.237628 => 06/10/2021
    # Jan 25, 2021 - cjfas-2020-0398.R1 => 01/25/2021

    test_dates = {
        '01/31/2021': "1/31/21",
        '01/04/2021': "1/4/21 20:00",
        '01/21/2021': "01/21/2021",
        '06/24/2021': "2021-06-24 21:18:29",
        '06/10/2021': "10-Jun-2021 - 10.1242/jeb.237628",
        '01/25/2021': "Jan 25, 2021 - cjfas-2020-0398.R1",
    }

    for key, val in test_dates.items():
        res = normalized_date(val)
        print("input date: {}".format(val))
        print("normalized: {}".format(res))
        assert(key == res)

def tests():
    test_normalized_date()
    test_remove_punctuation()

if __name__ == "__main__":
    tests()

