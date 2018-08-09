"""
Make config files by accession and run name (ex Tobacco 14 Shotgun).
Extract barcode from sample registry and generate config file for dnabc
Chunyu Zhao 2018-07-17
"""
import os
import sys
import collections
import ruamel.yaml
import sqlite3
import json
import csv
import argparse
from collections import defaultdict


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
        data_uri = [x for x in data_uri if not "170424_D00727_0029_ACA2VDANXX" in x][0]
        lanes = "12345678"  # <- fine hard coded here
    elif run_num == "5":
        # print(data_uri)
        ## find just hard coded and manually run twice for run5
        data_uri = data_uris[0]
        lanes = "345678"
    return data_uri, lanes


def mk_data_path(sql_fifth_col):
    """return
        /mnt/isilon/microbiome/raw_data/170424_D00727_0029_ACA2VDANXX or
        /mnt/isilon/microbiome/Hiseq02/160901_D00728_0028_BC9W1KANXX/Data/Intensities/BaseCalls
    """
    uri = os.path.dirname(sql_fifth_col)
    if "_L00" in uri:
        uri = uri.rsplit("_L00")[0]
    if uri.startswith("Hiseq01") or uri.startswith("Hiseq02"):
        uri = os.path.join("/mnt/isilon/microbiome/", uri)
    return uri


def mk_one_config(project_dir, experiment, run_num, c, conn):
    run_name = experiment + " " + str(run_num)

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
        data_uri, lanes = handle_diff_dir_lanes(run_num, data_uri, data_uris)
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
    barcodes_fp = os.path.join(
        args.project_dir, "barcodes_" + run_name.replace(" ", "") + ".txt"
    )
    with open(barcodes_fp, "w") as file:
        for key, value in barcodes.items():
            file.write("%s\t%s\n" % (key, value))
    conn.close()

    ## Read in default config yaml file
    with open("default_config.yml") as stream:
        config = ruamel.yaml.safe_load(stream)

    ## Update the corresonding values
    project_dir_fp = os.path.join(
        args.project_dir, "dynamic", run_name.replace(" ", "")
    )
    new = dict(
        [
            ("raw_data_fp", data_uri),
            ("lane_num", lanes),
            ("project_fp", project_dir_fp),
            ("project_dir", project_dir),
            ("barcodes", barcodes_fp),
        ]
    )
    config = _update_dict(config, new)

    ## Dump to a new config file
    new_config_file = "dnabc_config_" + run_name.replace(" ", "") + ".txt"
    with open(new_config_file, "w") as out:
        dump(config, out)


def parse_accession_info(row):
    lane = row['lane']
    acc = row['data_uri']
    if "microbiome/raw_data/" in acc:
        acc = "_".join(acc.split("raw_data/")[1].split("/")[0].split("_")[:-1])
    elif "Data/Intensities" in acc:
        acc = acc.split("/Data/")[0].split("/")[-1]
    else:
        raise ValueError("Unrecognized data dir path")

    return acc, mk_data_path(row['data_uri']), lane


def use_accession_and_lane(row, accession, lane):
    comment = row['comment']
    run = comment.split()[1]
    if "faulty" in comment:
        return False
    if "160901_D00728_0028_BC9W1KANXX" == accession and run == "5":
        return False
    if "Tobacco whole" in comment:
        # no data for Tobacco whole genome C. diff yet
        return False
    return True


def mk_barcodes(c, sql_query, fq_path):
    c.execute("SELECT * FROM runs_samples WHERE run_comment like ?", (sql_query,))
    barcodes = dict()
    for row in c:
        sample_name = row[1]
        barcode_seq = row[2]
        this_fq_path = [x for x in row if ".gz" in str(x)][0]
        assert row[9] == this_fq_path
        if this_fq_path == fq_path:
            barcodes[sample_name] = barcode_seq.replace("-", "")
    return barcodes


def write_barcodes(barcodes, out_file):
    with open(out_file, "w") as file:
        for key, value in barcodes.items():
            file.write("%s\t%s\n" % (key, value))


def print_configs(project_dir, experiment, key_to_lane, key_to_data_dir, barcodes):
    """one config per key (accession + '__' + runNameNoSpaces)"""
    barcode_files = []
    config_files = []
    for key in key_to_lane:
        new_config_file = "dnabc_config_%s.txt" % (key,)
        config_files.append(new_config_file)
        project_dir_fp = os.path.join(args.project_dir, "dynamic", experiment, key)
        barcode_file = os.path.join(project_dir, "barcodes.%s.txt" % (key,))
        write_barcodes(barcodes[key], barcode_file)
        barcode_files.append(barcode_file)
        new = dict(
            [
                ("raw_data_fp", key_to_data_dir[key]),
                ("lane_num", "".join([str(x) for x in key_to_lane[key]])),
                ("project_fp", project_dir_fp),
                ("project_dir", project_dir),
                ("barcodes", barcode_file),
            ]
        )

        ## Read in default config yaml file
        with open("default_config.yml") as stream:
            config = ruamel.yaml.safe_load(stream)

        config = _update_dict(config, new)
        with open(new_config_file, "w") as out:
            dump(config, out)
    with open("run_configs.sh", "w") as fout:
        for config_file in config_files:
            cmd = "snakemake --configfile %s -s Snakefile -j50 all_dnabc" % (
                config_file,
            )
            print(cmd, file=fout)
    return barcode_files


def init_dict():
    return defaultdict(int)


def check_barcodes(barcode_files):
    """Count number of barcode names by run name.
       If a barcode names appears more than three times
       for a run name, there's a problem.
    """
    name_counts = defaultdict(init_dict)
    for afile in barcode_files:
        run_name = afile.split("__")[1].split(".")[0]
        with open(afile) as f:
            for line in f:
                bc_name, _ = line.strip().split("\t")
                name_counts[run_name][bc_name] += 1

    with open("barcode_test", "w") as fout:
        for run_name in name_counts:
            for bc_name in name_counts[run_name]:
                if name_counts[run_name][bc_name] > 1:
                    print(
                        "\t".join(
                            [
                                str(x)
                                for x in (
                                    run_name,
                                    bc_name,
                                    name_counts[run_name][bc_name],
                                )
                            ]
                        ),
                        file=fout,
                    )


def mk_multiple_configs(project_dir, experiment, c, conn, run_table_cols):
    """ Mk multiple snakemake configs for an experiment.
        Check that each run and barcode appear no more than twice.

    Args:
        project_dir: dir that will hold data
        experiment: query name for experiment (ex Tobacco)
        c: sqlite connection cursor
        conn: sqlite connection
    """

    # look for any runs for this experiment
    sql_query = "%" + experiment + " %"
    c.execute("SELECT * FROM runs WHERE comment like ?", (sql_query,))

    # key is accession + __ runNameNoSpaces
    key_to_lane, key_to_data_dir = defaultdict(set), {}
    key_to_lane_queries = defaultdict(list)
    key_to_accession = {}
    barcodes = {}
    for db_row in c:
        row = {col:val for col, val in zip(run_table_cols, db_row)}
        accession, data_dir, lane = parse_accession_info(row)
        if use_accession_and_lane(row, accession, lane):
            run_name = row['comment']
            use_run_name = run_name.replace(" ", "")
            if "Tobacco 8 Shotgun and FARMM5 redos" == run_name:
                use_run_name = "Tobacco8Shotgun"

            key = accession + "__" + use_run_name
            key_to_lane[key].add(lane)
            key_to_data_dir[key] = data_dir
            key_to_accession[key] = accession
            sql_query = "%" + run_name + "%"
            fq_path = row['data_uri']
            key_to_lane_queries[key].append((fq_path, sql_query))
    for key in key_to_lane_queries:
        for fq_path, sql_query in key_to_lane_queries[key]:
            barcodes[key] = mk_barcodes(c, sql_query, fq_path)
    barcode_files = print_configs(
        project_dir, experiment, key_to_lane, key_to_data_dir, barcodes
    )
    check_barcodes(barcode_files)


def main(args):
    run_num = args.run_num
    conn = sqlite3.connect(args.db)
    c = conn.cursor()

    c.execute('PRAGMA TABLE_INFO(runs)')
    run_table_cols = [x[1] for x in c.fetchall()]

    if run_num == "0":
        mk_multiple_configs(args.project_dir, args.exp, c, conn, run_table_cols)
    else:
        mk_one_config(args.project_dir, args.exp, run_num, c, conn)


if __name__ == "__main__":
    desc = "Mk config for demultiplexing."
    ap = argparse.ArgumentParser(description=desc)
    ap.add_argument(
        "-n",
        "--run-num",
        dest="run_num",
        help="Run number",
        required=False,
        default="0",
    )
    ap.add_argument(
        "-e", "--exp", help="Experiment name", required=False, default="Tobacco"
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
