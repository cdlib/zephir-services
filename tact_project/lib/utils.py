import os
import sys

import sqlalchemy.engine.url
import yaml
from decimal import Decimal

def db_connect_url(config):
    """Database connection URL creates a connection string through configuration
    values passed. The method allows for environmental variable overriding.

    Notes: These strings depend on the sqlalchemy package.

    Args:
        config:  A dictionary of database configuration values.

    Returns:
        A database connection string compatable with sqlalchemy.

        """
    drivername = config.get("drivername")
    username = config.get("username")
    password = config.get("password")
    host = config.get("host")
    port = config.get("port")
    database = config.get("database")

    url = str(
        sqlalchemy.engine.url.URL(drivername, username, password, host, port, database)
    )

    return url

def get_configs_by_filename(config_file):
    """return configs defined in the config_file

    Args:
      config_file: full path of the yaml config file
    Returns:
      dict of configurations
    """

    configs = {}
    with open(config_file, 'r') as ymlfile:
        configs = yaml.safe_load(ymlfile)

    return configs



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
    test_srt_to_decimal()
    test_multiple_doi()

if __name__ == "__main__":
    tests()
