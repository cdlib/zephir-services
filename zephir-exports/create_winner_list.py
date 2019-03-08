import pymarc
from sqlalchemy import create_engine
import zlib

with open("outcome1.tsv", "a") as outcome_file:

    engine = create_engine("sqlite:///cache/complete-gz.db", echo=False)
    with engine.connect() as con:
        create_table_stmt = "select cache_data from cache"
        result = con.execute(create_table_stmt)
        for idx, row in enumerate(result):
            for marc_record in pymarc.JSONReader(
                zlib.decompress(row[0]).decode("utf8")
            ):
                cid = marc_record["CID"]["a"]
                htid = marc_record["HOL"]["p"]
                records = str(len(marc_record.get_fields("974")))
                outcome_file.write(cid + "\t" + htid + "\t" + records + "\n")
