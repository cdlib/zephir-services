import sys

if (len(sys.argv) != 2):
    print("Parameter error.")
    print("Usage: {} cid_filename".format(sys.argv[0]))
    exit(1)

filename = sys.argv[1]

count = 0
cids = {}
with open(filename) as fp:
   for line in fp:
        count += 1
        #print("Line {}: {}".format(count, line.strip()))
        x = line.split(": ")
        #print ("item={}: cid={}".format(x[0], x[1]))
        key = x[0].strip()
        value = x[1].strip()
        if key in cids.keys():
            #print ("Key has defined: {} {}".format(key, value))
            if cids[key] != value:
                print ("Different CIDs: item={} : {} : {}".format(key, cids[key], value))
        else:
            #print ("Not in, define new key/value: {} {}".format(key, value))
            cids[key] = value
