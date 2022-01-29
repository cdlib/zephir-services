from decimal import Decimal
from datetime import datetime

def str_to_decimal(a_str):
    try:
        return Decimal(a_str.replace(',','').replace('$', ''))
    except:
        return 0

def multiple_doi(input_str):
    """Check if input string contains multiple DOIs - Document Object Identifier
    
    Check if input string contains sub-strings separated by a space or newline.
    DOI syntax (prefix/suffix) is not checked.

    Args:
        input_str: a string

    Returns:
        True: when input string contains sub-strings separated by a space or newline
        False: otherwise

    """
    check_list = [" ", "\n"]

    if input_str:
        return any(item in input_str.rstrip("\n").strip() for item in check_list)
    else:
        return False

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

def test_multiple_doi():

    doi=None
    res = multiple_doi(doi)
    print("DOI: {} Multiple?: {}".format(doi, res))
    assert(res == False)

    doi=" "
    res = multiple_doi(doi)
    print("DOI: {} Multiple?: {}".format(doi, res))
    assert(res == False)

    doi = ""
    res = multiple_doi(doi)
    print("DOI: {} Multiple?: {}".format(doi, res))
    assert(res == False)

    doi = "abc"
    res = multiple_doi(doi)
    print("DOI: {} Multiple?: {}".format(doi, res))
    assert(res == False)

    doi = "10.1017/lap.2018.77"
    res = multiple_doi(doi)
    print("DOI: {} Multiple?: {}".format(doi, res))
    assert(res == False)

    doi = "cjfas-2020-0398.R1"   # CSP doi
    res = multiple_doi(doi)
    print("DOI: {} Multiple?: {}".format(doi, res))
    assert(res == False)

    doi = "http://dx.doi.org/10.1145/3418498"
    res = multiple_doi(doi)
    print("DOI: {} Multiple?: {}".format(doi, res))
    assert(res == False)

    doi = "https://doi.org/10.1145/3290605.3300263"
    res = multiple_doi(doi)
    print("DOI: {} Multiple?: {}".format(doi, res))
    assert(res == False)

    doi = """10.1017/S0165115319000597
10.1017/S0165115319000603
10.1017/S0165115319000743
10.1017/S0165115319000755
10.1017/S0165115319000767
"""
    res = multiple_doi(doi)
    print("DOI: {} Multiple?: {}".format(doi, res))
    assert(res == True)

    doi = "10.1017/S0165115319000597 10.1017/S0165115319000603"
    res = multiple_doi(doi)
    print("DOI: {} Multiple?: {}".format(doi, res))
    assert(res == True)

    doi = "10.1017/S0165115319000597\n10.1017/S0165115319000603"
    res = multiple_doi(doi)
    print("DOI: {} Multiple?: {}".format(doi, res))
    assert(res == True)

def test_srt_to_decimal():
    a_str = ""
    print(str_to_decimal(a_str))
    assert(str_to_decimal(a_str) == 0)

    a_str = " "
    print(str_to_decimal(a_str))

    a_str = "   "
    print(str_to_decimal(a_str))

    a_str = None 
    print(str_to_decimal(a_str))

    a_str = " $10,000.01"
    print(str_to_decimal(a_str))

def tests():
    #test_srt_to_decimal()
    #test_multiple_doi()
    test_normalized_date()

if __name__ == "__main__":
    tests()
