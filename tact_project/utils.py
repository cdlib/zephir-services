from decimal import Decimal

def str_to_decimal(a_str):
    try:
        return Decimal(a_str.replace(',','').replace('$', ''))
    except:
        return 0

def test():
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

if __name__ == "__main__":
    test()
