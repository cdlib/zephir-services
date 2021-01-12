import sys
import os

if (len(sys.argv) != 2):
    print("Parameter error.")
    print("Usage: {} cid_filename".format(sys.argv[0]))
    exit(1)

input_file = sys.argv[1]
outpuf_file = os.path.splitext(input_file)[0] + "_rpt.txt"

output = open(outpuf_file, "w")
output.write("category:run_report_filename:sys_id:new_cid:old_cid\n")

count = 0
cids = {}
with open(input_file) as fp:
   for line in fp:
        count += 1
        #print("Line {}: {}".format(count, line.strip()))
        x = line.split(":")
        if (len(x) ==5):
            run_report = x[0]
            key_item = x[1].strip()
            value_cid = x[2].strip()
        else:
            run_report = ""
            key_item = x[0].strip()
            value_cid = x[1].strip()
        #print ("run_report={}: item={}: cid={}".format(run_report, key_item, value_cid))

        if key_item in cids.keys():
            #print ("Key has defined: {} {}".format(key_item, value_cid))
            if cids[key_item] != value_cid:
                print ("Different CIDs:{}:item={}:new={}:old={}".format(run_report, key_item, cids[key_item], value_cid))
                output.write("Different CIDs:{}:{}:{}:{}\n".format(run_report, key_item, cids[key_item], value_cid))
        else:
            #print ("Not in, define new key_item/value_cid: {} {}".format(key_item, value_cid))
            cids[key_item] = value_cid

output.write("\n")
output.write("Total processed sys IDs:{}\n".format(len(cids)))
output.close()
