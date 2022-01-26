import os

from csv import DictReader

def load_csv(filename):
    output = []
    with open(filename, 'r', newline='', encoding='UTF-8-sig') as csvfile:
        fieldnames = ['title1', 'subtitle1', 'author1', 'publisher1', 'language1', 'year published1',
                      'title2', 'subtitle2', 'author2', 'publisher2', 'language2', 'year published2']
        outdicts = DictReader(csvfile, fieldnames=fieldnames)
        #outdicts = DictReader(csvfile)
        for row in outdicts:
            print(row)
            output.append(row)

    return output

def print_records():
    filename = "./data/zephir_test_records_1.csv" # without filednames
    #filename = "./data/zephir_test_records_2.csv"  # with fieldnames
    record_data = load_csv(filename)
    for row in record_data:
        title1= row['title1'] + row['subtitle1']
        author1 = row['author1']
        publisher1 = row['publisher1']
        lang1 = row['language1']
        year1 = row['year published1']

        title2 = row['title2'] + row['subtitle2']
        author2 = row['author2']
        publisher2 = row['publisher2']
        lang2 = row['language2']
        year2 = row['year published2']

        data = [title1, author1, publisher1, lang1, year1, title2, author2, publisher2, lang2, year2]
        print(data)


if __name__ == "__main__":
    # execute only if run as a script
    print_records()


