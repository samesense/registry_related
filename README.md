# registry_related

From [sample registry](http://reslnmbiomea01.research.chop.edu/registry/), generate the config file for demultiplex using [dnabc_sunbeam](https://github.com/zhaoc1/dnabc_sunbeam).

## Create conda environment
```
conda env create --name=dnabc --file environment.yml 

# conda only keeps track of the packages it installed. 
pip install git+git://github.com/zhaoc1/dnabc.git
```

## Usage
* Create config for specific run (Ex Tobacco 9 Shotgun)
```
python config.py -n 9 -d /mnt/isilon/microbiome/registry_backup/core.db_20180627 -p /home/evansj/me/projects/bittinger/dat
```
* Create one config for each accession in an experiment group (Ex Tobacco)
```
python config.py -e Tobacco -d /mnt/isilon/microbiome/registry_backup/core.db_20180627 -p /home/evansj/me/projects/bittinger/dat

# config.py will make run_configs.sh
# use that to for Snakemake runs
sh run_configs.sh

# Collapse runs 14 and 15
snakemake --configfile configs/combine14_config.json -s Snakefile_combine.py all_combine
snakemake --configfile configs/combine15_config.json -s Snakefile_combine.py all_combine
```

## Notes
For recent tobacco runs, the pooled DNA got sequenced in multiple Hiseq runs, and we need to merge the files after dnabc them separately.

