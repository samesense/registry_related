####
# demultiplex undetermined reads
# author: Chunyu Zhao
# time: 07-19-2018
####

import glob
import pathlib
import random
import configparser
import os
import sys
import shutil
import yaml
import subprocess
import time
import json 

def read_samples_from_barcodes_json(barcode):
    with open(barcode) as file:
        samples = json.load(file)
    return(samples.keys())

def read_samples_from_barcodes(barcode):
    with open(barcode) as file:
        lines = file.read().splitlines()
    samples = []
    for line in lines:
        samples.append(line.split('\t')[0])
    return(samples)

#print(config['barcodes'])
BARCODE_FP = config["barcodes"]
LANES = list(config["lane_num"])
SAMPLE_IDS = read_samples_from_barcodes(BARCODE_FP)

DNABC_FP = config["project_fp"] + "/" + config["output"]["dnabc"]
TARGET_FPS = expand(DNABC_FP + "/{sample}_{read}.fastq", sample=SAMPLE_IDS, read=["R1","R2"])


workdir: config["project_fp"]
starttime = int(time.time())

rule all:
    input: 
        expand(DNABC_FP + "/{sample}_{read}.fastq.gz", sample=SAMPLE_IDS, read=["R1","R2"])

rule all_dnabc:
    input:
        expand(DNABC_FP + "/{sample}_{read}.fastq.gz", sample=SAMPLE_IDS, read=["R1","R2"])

def mk_input_for_copy_file(wildcards):
    test_path = config["raw_data_fp"] + "/Undetermined_S0_L00%s_%s_001.fastq.gz" % (wildcards.lane, wildcards.rp)
    if os.path.exists(test_path):
       return test_path

    test_path = config["raw_data_fp"] + "_L00%s/Undetermined_S0_L00%s_%s_001.fastq.gz" % (wildcards.lane, wildcards.lane, wildcards.rp)
    if os.path.exists(test_path):
       return test_path

# rule copy_file:
#     input:
#         mk_input_for_copy_file
#     output:
#         temp(config["project_fp"] + "/" + "Undetermined_S0_L00{lane}_{rp}_001.fastq.gz")
#     params:
#         config["project_fp"]
#     shell:
#         """
#         cp {input[0]} {params[0]}
#         """

rule gunzip_file:
    input:
        mk_input_for_copy_file #    config["project_fp"] + "/" + "Undetermined_S0_L00{lane}_{rp}_001.fastq.gz"
    output:
        temp(config["project_fp"] + "/" + "Undetermined_S0_L00{lane}_{rp}_001.fastq")
    shell:
       "gunzip -c {input[0]} > {output[0]}"

rule cat_R1s:
    input:
        expand(config["project_fp"] + "/" + "Undetermined_S0_L00{lane}_R1_001.fastq", lane=list(config["lane_num"]))
    output:
        temp(config["project_fp"] + "/" + "Undetermined_S0_L" + config["lane_num"] + "_R1_001.fastq")
    shell:
       "cat {input} > {output[0]}"

rule cat_R2s:
    input:
        expand(config["project_fp"] + "/" + "Undetermined_S0_L00{lane}_R2_001.fastq", lane=list(config["lane_num"]))
    output:
        temp(config["project_fp"] + "/" + "Undetermined_S0_L" + config["lane_num"] + "_R2_001.fastq")
    shell:
       "cat {input} > {output[0]}"

rule demultiplex:
    input:
        read1 = config["project_fp"] + "/Undetermined_S0_L" + config["lane_num"] + "_R1_001.fastq",
        read2 = config["project_fp"] + "/Undetermined_S0_L" + config["lane_num"] + "_R2_001.fastq"
    output:
        temp(TARGET_FPS)
    params:
        dnabc_summary = DNABC_FP + "/summary-dnabc.json"
    log: 
        DNABC_FP + "/dnabc.log"
    threads:
        config['threads']
    shell:
        """
        dnabc.py --forward-reads {input.read1} --reverse-reads {input.read2} \
        --barcode-file {BARCODE_FP} --output-dir {DNABC_FP} \
        --summary-file {params.dnabc_summary} &> {log}
        """

rule gzip_files:
     input:
        DNABC_FP + "/{filename}.fastq"
     output:
        DNABC_FP + "/{filename}.fastq.gz"
     shell:
        """
        gzip {input}
        """

onsuccess:
	print("Workflow finished, no error")
	shell("mail -s 'workflow finished' " + config['admins']+" <{log}")
onerror:
	print("An error occurred")
	shell("mail -s 'an error occurred' " + config['admins']+" < {log}")
