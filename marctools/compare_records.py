from fuzzywuzzy import fuzz
from fuzzywuzzy import process

class FuzzyRatios:
    def __init__ (self, string_1, string_2):
        self.str_1 = string_1.lower() if string_1 else ""
        self.str_2 = string_2.lower() if string_2 else ""
        self.fuzzy_ratio = fuzz.ratio(self.str_1, self.str_2)
        self.fuzzy_partial_ratio = fuzz.partial_ratio(self.str_1, self.str_2)
        self.fuzzy_token_sort_ratio = fuzz.token_sort_ratio(self.str_1, self.str_2)
        self.fuzzy_token_set_ratio = fuzz.token_sort_ratio(self.str_1, self.str_2)

    @staticmethod
    def match_strings(string_1, string_2):
        """
        Levenshtein distances with fuzzywuzzy
        """
        if not string_1 or not string_2:
            return ''
        ratio = fuzz.ratio(string_1.lower(), string_2.lower())
        if ratio >= 90:
            return True
        ratio_partial = fuzz.partial_ratio(string_1.lower(), string_2.lower())
        if ratio_partial >= 90:
            return True
        return False

def test():
    str_a = "a"
    str_b= "b"
    compare = FuzzyRatios(str_a, str_b)
    print (str_a)
    print (str_b)
    print ("fuzzy_ratio {}".format(compare.fuzzy_ratio))
    print ("fuzzy_partial_ratio {}".format(compare.fuzzy_partial_ratio))
    print ("fuzzy_token_sort_ratio {}".format(compare.fuzzy_token_sort_ratio))
    print ("fuzzy_token_set_ratio {}".format(compare.fuzzy_token_set_ratio))

    str_a = "a"
    str_b = "a "
    compare = FuzzyRatios(str_a, str_b)
    print (str_a)
    print (str_b)
    print ("fuzzy_ratio {}".format(compare.fuzzy_ratio))
    print ("fuzzy_partial_ratio {}".format(compare.fuzzy_partial_ratio))
    print ("fuzzy_token_sort_ratio {}".format(compare.fuzzy_token_sort_ratio))
    print ("fuzzy_token_set_ratio {}".format(compare.fuzzy_token_set_ratio))

    str_a = "APPLE"
    str_b = "apple "
    compare = FuzzyRatios(str_a, str_b)
    print (str_a)
    print (str_b)
    print ("fuzzy_ratio {}".format(compare.fuzzy_ratio))
    print ("fuzzy_partial_ratio {}".format(compare.fuzzy_partial_ratio))
    print ("fuzzy_token_sort_ratio {}".format(compare.fuzzy_token_sort_ratio))
    print ("fuzzy_token_set_ratio {}".format(compare.fuzzy_token_set_ratio))

    str_a = 'FuzzyWuzzy is a lifesaver!'
    str_b = 'fuzzy wuzzy is a LIFE SAVER.'
    ratio = fuzz.ratio(str_a.lower(), str_b.lower())
    print('Similarity score: {}'.format(ratio))

    str_a = 'Chicago, Illinois'
    str_b = 'Chicago'
    ratio = fuzz.partial_ratio(str_a.lower(), str_b.lower())
    print('Similarity score: {}'.format(ratio))

    str_a = 'Chicago, Illinois'
    str_b = 'Chicago'
    ratio = fuzz.partial_ratio(str_b.lower(), str_a.lower())
    print('Similarity score: {}'.format(ratio))

    str_a = 'Gunner William Kline' 
    str_b = 'Kline, Gunner William'
    ratio = fuzz.token_sort_ratio(str_a, str_b)
    print('Similarity score: {}'.format(ratio))


    str_a = 'The 3000 meter steeplechase winner, Soufiane El Bakkali'
    str_b = 'Soufiane El Bakkali'
    ratio = fuzz.token_set_ratio(str_a, str_b)
    print('Similarity score: {}'.format(ratio))


    choices = ["3000m Steeplechase", "Men's 3000 meter steeplechase",
            "3000m STEEPLECHASE MENS", "mens 3000 meter SteepleChase"]
    process.extract("Men's 3000 Meter Steeplechase", choices, scorer=fuzz.token_sort_ratio)


    choices = ["3000m Steeplechase", "Men's 3000 meter steeplechase",
            "3000m STEEPLECHASE MENS", "mens 3000 meter SteepleChase"]
    process.extractOne("Men's 3000 Meter Steeplechase", choices, scorer=fuzz.token_sort_ratio)

def main():
    test()



if __name__ == "__main__":
    main()
