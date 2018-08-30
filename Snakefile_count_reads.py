"""How many reads in each fq file?
   Runs 14, 15, and 5 are special cases
"""
import os
import glob
import yaml

def parse_fq_paths(fp, barcode_file):
    files = []
    with open(barcode_file) as f:
        for line in f:
            name, bc = line.strip().split('\t')
            path = os.path.join(fp, 'dnabc_results/', name + '.fastq.gz')
            #print(path)
            #if os.path.exists(path):
            files.append(os.path.join(fp, 'dnabc_results/', name + '_R1'))
            files.append(os.path.join(fp, 'dnabc_results/', name + '_R2'))
    return files


def parse_barcode(config_file):
    """5, 14, 15 are special cases where two runs were combined"""
    with open(config_file) as y:
        config = yaml.load(y)
        paths = parse_fq_paths(config['project_fp'], config['barcodes'])
        paths_combine = []
        if '180501_D00728_0061_ACC8V6ANXX__Tobacco14Shotgun' in config_file:
            paths_combine = parse_fq_paths('/'.join(config['project_fp'].split('/')[:-2]) + '/Tobacco/combine__Tobacco14Shotgun', config['barcodes'])
        elif '180501_D00728_0061_ACC8V6ANXX__Tobacco15Shotgun' in config_file:
            paths_combine = parse_fq_paths('/'.join(config['project_fp'].split('/')[:-2]) + '/Tobacco/combine__Tobacco15Shotgun', config['barcodes'])
        elif 'dnabc_config_160901_D00728_0028_BC9W1KANXX__Tobacco5Shotgun.txt' in config_file:
            paths_combine = parse_fq_paths('/'.join(config['project_fp'].split('/')[:-2]) + '/Tobacco/combine__Tobacco5Shotgun', config['barcodes'])
        return paths + paths_combine


def load_fq_files():
    """Load fq file paths w/o fastq.gz derived from configs"""
    files = []
    for config_file in glob.glob('./dnabc_config_*'):
        files.extend(parse_barcode(config_file))
    return files

FQS = load_fq_files()
print(FQS[:5])
rule count_reads:
    input:  '{afile}.fastq.gz'
    output: '{afile}.read_count'
    shell:
        'zcat {input} | wc -l > {output}'

rule count_all_reads:
    input: expand('{afile}.read_count', afile=FQS)
    output: o = 'read_counts.csv'
    run:
        with open(output.o, 'w') as fout:
            print('group\tsample\treads', file=fout)
            for afile in input:
                run = afile.split('/Tobacco/')[1].split('/')[0]
                with open(afile) as f:
                    lines = int(f.readline().strip())/4
                    name = afile.split('/')[-1].split('.read_count')[0]
                    line = run + '\t' + name + '\t' + str(lines)
                    print(line, file=fout)
