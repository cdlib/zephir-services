import csv
import json

cid_htid={}
input_file = "cid_htid.txt"
with open(input_file) as my_file:
    reader = csv.reader(my_file, delimiter='\t')
    for row in reader:
        print("cid: {}, htid: {}".format(row[0], row[1]))
        cid_htid[row[0]] = row[1]

cid_oclc={}
input_file = "cid_oclc.txt"
with open(input_file) as my_file:
    reader = csv.reader(my_file, delimiter='\t')
    for row in reader:
        key = row[0]
        value = row[1]
        print("cid: {}, ocn: {}".format(key, value))
        if key in cid_oclc.keys():
            cid_oclc[key].add(value)
        else:
            cid_oclc[key] = {value}

for k, v in cid_oclc.items():
    print("cid: {} ocn: {}".format(k, v))

cid_sysid={}
input_file = "cid_sysid.txt"
with open(input_file) as my_file:
    reader = csv.reader(my_file, delimiter='\t')
    for row in reader:
        key = row[0]
        value = row[1]
        print("cid: {}, sysid: {}".format(key, value))
        if key in cid_sysid.keys():
            cid_sysid[key].add(value)
        else:
            cid_sysid[key] = {value}

for k, v in cid_sysid.items():
    print("cid: {} sysid: {}".format(k, v))

cid_ids={}
for k, v in cid_htid.items():
    cid_ids[k] = {
            "cid": k,
            "htid": v,
            "ocns": cid_oclc[k],
            "sysid": cid_sysid[k]}

with open("cid_ids.json", 'w') as out_file:
    for k, v in cid_ids.items():
        print("cid: {} Json: {}".format(k, v))
        out_file.write(str(v))



