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
            key = x[1].strip()
            value = x[2].strip()
        else:
            run_report = ""
            key = x[0].strip()
            value = x[1].strip()
        #print ("run_report={}: item={}: cid={}".format(run_report, key, value))

        if key in cids.keys():
            #print ("Key has defined: {} {}".format(key, value))
            if cids[key] != value:
                print ("Different CIDs:{}:item={}:new={}:old={}".format(run_report, key, cids[key], value))
                output.write("Different CIDs:{}:{}:{}:{}\n".format(run_report, key, cids[key], value))
        else:
            #print ("Not in, define new key/value: {} {}".format(key, value))
            cids[key] = value

output.write("\n")
output.write("Total processed sys IDs:{}\n".format(len(cids)))
output.close()
