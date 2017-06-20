#!/bin/bash

import pprint

files = [ 'source_count_test12.log', 'dest_count_test12.log' ]
data = {}

overall_source_count = 0
overall_dest_count = 0
for file in files:
    print ("file=%s" % file)
    with open(file) as f:
        content = f.readlines()
        for line in content:
            l = line.split(' ')
            ns = l[1].strip()
            sss= l[0]
            sc = sss.split('=')
            count = sc[1]
            key = 'source'
            if file=='dest_count_test12.log':
                key = "dest"
                overall_dest_count += int(count)
            else:
                overall_source_count += int(count)
            if not ns in data:
                data[ns] = {}
            data[ns][key]=count
    print("osc=%i odc=%i" % (overall_source_count, overall_dest_count))


    print("osc=%i odc=%i" % (overall_source_count, overall_dest_count))
    pprint.pprint(data)

print("\nOverall Source Count:%i" % overall_source_count)
print("Overall Destination Count:%i" % overall_dest_count)
print("Diff: %i\n" % (overall_source_count - overall_dest_count))

print("Collections with different counts")
print("---------------------------------")

for ns in data:
    if not data[ns]['source'] == data[ns]['dest']:
        diff = int(data[ns]['source']) - int(data[ns]['dest'])
        print "%s (%i) %s" % (ns,diff,str(data[ns]))
