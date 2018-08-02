import os
import sys
import collections
import ruamel.yaml
import sqlite3
import json
import csv

# extract barcode from sample registry and generate config file for dnabc
# Chunyu Zhao 2018-07-17


def _update_dict(target, new):
	for k, v in new.items():
		if isinstance(v, collections.Mapping):
			if k in target:
				target[k] = _update_dict(target[k], v)
			else:
				target[k] = _update_dict({}, v)
		else:
			target[k] = v
	return target

def dump(config, out=sys.stdout):
	if isinstance(config, collections.Mapping):
		ruamel.yaml.round_trip_dump(config, out)
	else:
		out.write(config)

# hard code the DB
conn = sqlite3.connect('core.db_20180627')
c = conn.cursor()

if len(sys.argv) == 2:
	run_num = sys.argv[1]
else:
	run_num = 1

run_name = 'Tobacco ' + str(run_num)
sql_query = '%'+ run_name +' %'
c.execute('SELECT * FROM runs WHERE comment like ?', (sql_query,))

lanes = []
data_uris = []
for row in c:
	lanes.append(row[4])
	## From run1 - run7: one format
	uri = os.path.dirname(row[5])
	if "_L00" in uri:
		## From run8 - now: another format...
		## I decide to move the `lane` information to snakemake
		data_uris.append(uri.rsplit("_L00")[0])
	else:
		data_uris.append(os.path.dirname(row[5]))
	

lanes = ''.join(map(str, lanes))
data_uri = list(set(data_uris))


## ugly codes for handling various situations....
if len(data_uri) != 1:
	print("ERROR: different lanes were not stored in the same base directory")
	if run_num == "9":
		## faulty flowcell
		data_uri = data_uri[1]
		lanes = "12345678" #<- fine hard coded here
	elif run_num == "5":
		print(data_uri)
		## find just hard coded and manually run twice for run5
		data_uri = data_uris[0]
		lanes = "345678"
		#data_uri = data_uris[1]
		#lanes = "34"
	else:
		exit(0)
else:
	data_uri = data_uri[0]
	if data_uri.startswith("Hiseq01"):
		data_uri = "/mnt/isilon/microbiome/" + data_uri
	if data_uri.startswith("Hiseq02"):
		data_uri = "/mnt/isilon/microbiome/" + data_uri

print("For %s, the lanes and data_uri informations are" % (run_name))
print(lanes)
print(data_uri)


## Get the sample and barcode information 
c.execute('SELECT * FROM runs_samples WHERE run_comment like ?', (sql_query,))

barcodes = dict()
for row in c:
	sample_name = row[1]
	barcode_seq = row[2]
	if sample_name not in barcodes:
		barcodes[sample_name] = barcode_seq.replace("-", "")

## dnabc.py required tab-delimited files
barcodes_fp = "barcodes_" + run_name.replace(" ","") + ".txt"
with open(barcodes_fp, "w") as file:
	for key, value in barcodes.items():
		file.write('%s\t%s\n' % (key,value))

conn.close()

## Read in default config yaml file
with open("default_config.yml") as stream:
	config = ruamel.yaml.safe_load(stream)

## Update the corresonding values
project_dir = "/scr1/users/zhaoc1/project/dynamic/" + run_name.replace(" ", "") #<- i hard coded my target dir here
new = dict([("raw_data_fp", data_uri), ("lane_num", lanes), 
	("project_fp", project_dir), ("barcodes", barcodes_fp)])
config = _update_dict(config, new)

## Dump to a new config file
new_config_file = "dnabc_config_" + run_name.replace(" ", "") + ".txt"
with open(new_config_file, 'w') as out:
	dump(config, out)