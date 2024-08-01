import mysql.connector
import argparse
from more_itertools import chunked
import dotenv
import os
from pprint import pprint

dotenv.load_dotenv()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", "-f", type=str, required=True)
    parser.add_argument("--output", "-o", type=str, required=True)

    args = parser.parse_args()

    with open(args.file, "r") as f:
        ids = f.read().splitlines()
        
    ids = list(chunked(ids, 100))

    mydb = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE")
    )

    mycursor = mydb.cursor()

    ids_exploded = set()

    for i in ids:
        
        mycursor.execute(f"""
        select id from zephir_records where cid in (
        select cid from zephir_records where id in {tuple(i)}
        )
        """)
        myresult = mycursor.fetchall()
        ids_exploded.update(myresult)

    with open(args.output, "w") as f:
        for i in ids_exploded:
            f.write(f"{i[0]}\n")


if __name__ == "__main__":
    main()
