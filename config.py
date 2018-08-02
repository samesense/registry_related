import os
import sys
import collections
import ruamel.yaml
import sqlite3
import json
import csv
import argparse

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


def handle_diff_dir_lanes(run_num, data_uri, data_uris):
    print("ERROR: different lanes were not stored in the same base directory")
    assert run_num in ("9", "5")
    if run_num == "9":
        ## faulty flowcell
        data_uri = data_uri[1]
        lanes = "12345678"  # <- fine hard coded here
    elif run_num == "5":
        print(data_uri)
        ## find just hard coded and manually run twice for run5
        data_uri = data_uris[0]
        lanes = "345678"
    return data_uri, lanes


def main(args):
    run_num = args.run_num

    # ex core.db_20180627
    conn = sqlite3.connect(args.db)
    c = conn.cursor()

    run_name = "Tobacco " + str(run_num)

    sql_query = "%" + run_name + " %"
    c.execute("SELECT * FROM runs WHERE comment like ?", (sql_query,))

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

    lanes = "".join(map(str, lanes))
    data_uri = list(set(data_uris))

    if len(data_uri) != 1:
        data_uri, lanes = handle_diff_dir_lanes(run_num, data_uri, data_urls)
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
    c.execute("SELECT * FROM runs_samples WHERE run_comment like ?", (sql_query,))

    barcodes = dict()
    for row in c:
        sample_name = row[1]
        barcode_seq = row[2]
        if sample_name not in barcodes:
            barcodes[sample_name] = barcode_seq.replace("-", "")

    ## dnabc.py required tab-delimited files
    barcodes_fp = os.path.join(args.project_dir, "barcodes_" + run_name.replace(" ", "") + ".txt")
    with open(barcodes_fp, "w") as file:
        for key, value in barcodes.items():
            file.write("%s\t%s\n" % (key, value))
    conn.close()

    ## Read in default config yaml file
    with open("default_config.yml") as stream:
        config = ruamel.yaml.safe_load(stream)

    ## Update the corresonding values
    project_dir_fp = os.path.join(args.project_dir, "dynamic", run_name.replace(" ", ""))
    new = dict(
        [
            ("raw_data_fp", data_uri),
            ("lane_num", lanes),
            ("project_fp", project_dir_fp),
            ("project_dir", args.project_dir),
            ("barcodes", barcodes_fp),
        ]
    )
    config = _update_dict(config, new)

    ## Dump to a new config file
    new_config_file = "dnabc_config_" + run_name.replace(" ", "") + ".txt"
    with open(new_config_file, "w") as out:
        dump(config, out)


if __name__ == "__main__":
    desc = "Mk config for demultiplex."
    ap = argparse.ArgumentParser(description=desc)
    ap.add_argument(
        "-n" "--run-num", dest="run_num", help="Run number", required=False, default=1
    )
    ap.add_argument("-d", "--db", dest="db", help="Path to sql db", required=True)
    ap.add_argument(
        "-p",
        "--project-dir",
        dest="project_dir",
        help="Path to project dir",
        required=True,
    )
    args = ap.parse_args()
    main(args)
