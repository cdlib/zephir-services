import os

import tensorflow as tf
import tensorflow_hub as hub
import numpy as np
from csv import DictReader

# download USE from tensoflow-hub
#module_url = "https://tfhub.dev/google/universal-sentence-encoder-large/5" 
#USE_model = hub.load(module_url)

# use locally saved model 
USE_model = tf.keras.models.load_model(os.path.abspath(os.path.join(os.getcwd(), './models/universal-sentence-encoder-large_5')))

compare_model = tf.keras.models.load_model(os.path.abspath(os.path.join(os.getcwd(), './models/comparison_transfer.model')))
compare_model.compile(loss=tf.keras.losses.BinaryCrossentropy(), optimizer='adam', metrics=['accuracy'])

def embedUSE(input):
    return USE_model(input)

def load_csv(filename):
    output = []
    with open(filename, 'r', newline='', encoding='UTF-8-sig') as csvfile:
        fieldnames = ['title1', 'subtitle1', 'author1', 'publisher1', 'language1', 'year published1',
                      'title2', 'subtitle2', 'author2', 'publisher2', 'language2', 'year published2']
        outdicts = DictReader(csvfile, fieldnames=fieldnames)
        for row in outdicts:
            output.append(row)

    return output

def print_records(filename):
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


def prep_data(filename):
    record_data = load_csv(filename)

    output_data = []

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
        vector_data = embedUSE(data)
        vector_data = vector_data.numpy()

        output_data.append(vector_data)


    return (np.array(output_data))


def test_USE():
    embeddings = embedUSE([
        "The quick brown fox jumps over the lazy dog.",
        "I am a sentence for which I would like to get its embedding"])

    print(embeddings)

def predict():
    records_file = os.path.join(os.getcwd(), "./data/zephir_test_records_1.csv")
    
    print_records(records_file)

    vectors = prep_data(records_file)
    for vector in vectors:
        print(vector)
        predict = compare_model.predict(vector)
        print(predict)

if __name__ == "__main__":
    # execute only if run as a script
    predict()


