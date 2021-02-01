import sys
import os

if (len(sys.argv) != 2):
    print("Parameter error.")
    print("Usage: {} cid_filename".format(sys.argv[0]))
    exit(1)

input_file = sys.argv[1]
outpuf_file = os.path.splitext(input_file)[0] + "_rpt.txt"

output = open(outpuf_file, "w")
output.write("Category:Run Report Filename:Contribsys_ID:CID from New Code:CID from Current Code\n")

count = 0
new_cids = []
old_cids = []
with open(input_file) as fp:
   for line in fp:
        count += 1
        if count % 2 == 0:
            old_cids.append(line)
        else:
            new_cids.append(line)

if (len(new_cids) != len(old_cids)):
    print ("Log entires are not paired properly")
    print ("Lines from old process: {}".format(len(old_cids)))
    print ("Lines from new process: {}".format(len(new_cids)))
    exit (1)

#print ("paired lines: {}".format(len(new_cids)))

counts = {
    "Same CID": 0,
    "Different CIDs": 0,
    "Current code did not find CID": 0,
    "Neither current nor new code find CID": 0,
    "ERROR": 0,
    }
cat = ""
for i in range(len(new_cids)):
    x = new_cids[i].split(":")
    y = old_cids[i].split(":")
    if (len(x) == 5):
        run_report = x[0]
        item = x[1].strip()
        new_cid = x[2].strip()
        old_cid = y[2].strip()
    else:
        run_report = ""
        item = x[0].strip()
        new_cid = x[1].strip()
        old_cid = y[1].strip()
    #print ("run_report={}: item={}: new_cid={}: old_cid={}".format(run_report, item, new_cid, old_cid))

    # data format is different for error line
    if "ERROR" in old_cids[i]:
        if (len(y) == 4):
            error_msg = y[2].strip() + ", " + y[3].strip()
        else:
            run_report = ""
            error_msg = y[1].strip() + ", " + y[2].strip()

        cat = "ERROR" 
        #print ("ERROR:{}:item={}:new={}:old={}".format(run_report, item, new_cid, error_msg))
        output.write("{}:{}:{}:{}:{}\n".format(cat, run_report, item, new_cid, error_msg))
    else:
        if new_cid != old_cid:
            if not old_cid:
                cat = "Current code did not find CID"
            else:
                cat = "Different CIDs"
        else:
            if not old_cid:
                cat = "Neither current nor new code find CID"
            else:
                cat = "Same CID"
        #print ("{}:{}:item={}:new={}:old={}".format(cat, run_report, item, new_cid, old_cid))
        output.write("{}:{}:{}:{}:{}\n".format(cat, run_report, item, new_cid, old_cid))

    counts[cat] += 1


output.write("\n")
for key, val in counts.items():
    output.write("Total {}:{}\n".format(key, val))

output.write("Total records processed:{}\n".format(len(new_cids)))
output.close()

print("Selected log entries are saved in: {}".format(input_file))
print ("Report: {}".format(outpuf_file))
