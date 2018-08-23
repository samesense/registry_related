"""Combine sequencing runs.
"""
import os
from functools import reduce

def get_files(adir):
    return set([afile.split('_R')[0] for afile in os.listdir(adir)
                if not 'log' in afile and not 'json' in afile])

def mk_fq_prefix():
    return reduce(set().union, [get_files(_ + 'dnabc_results/') for _ in config['DIRS']])

rule combine_fq:
    input:
        expand('{dir}dnabc_results/{{afile}}.fastq.gz', dir=config['DIRS'])
    output:
        config['DATA_NEW'] + 'dnabc_results/{afile}.fastq.gz'
    shell:
        "zcat {input} | bgzip > {output}"

FILES = mk_fq_prefix()
rule all_combine:
    input:
        expand(config['DATA_NEW'] + 'dnabc_results/{afile}_R{read}.fastq.gz', afile=FILES, read=(1,2))
