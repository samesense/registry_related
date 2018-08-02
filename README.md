# registry_related

From [sample registry](http://reslnmbiomea01.research.chop.edu/registry/), generate the config file for [demultiplex](https://github.com/zhaoc1/dnabc_sunbeam).

## Create conda environment
conda env create --name=dnabc --file environment.yml 

## Notes
For recent tobacco runs, the pooled DNA got sequenced in multiple Hiseq runs, and we need to merge the files after dnabc them separately.

