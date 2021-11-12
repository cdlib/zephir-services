from decimal import Decimal

def str_to_decimal(a_str):
    try:
        return Decimal(a_str)
    except:
        return 0

