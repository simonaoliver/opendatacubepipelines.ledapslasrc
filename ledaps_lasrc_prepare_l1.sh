#!/bin/bash
#
#PBS -P v10
#PBS -q normal
#PBS -l walltime=4:00:00,ncpus=16,mem=64GB
#PBS -l wd
#PBS -me

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles/
module load gaip/dev-sen2redo
#module load agdc-py3-prod/1.5.1
module load parallel

HOME=/g/data/v10/AGDCv2/indexed_datasets/ledaps_lasrc/opendatacubepipelines.ledapslasrc
DATA=/g/data/v10/projects/ARD_interoperability/L1

find $DATA/TARGET -name *_MTL.txt | parallel --jobs 16 "python $HOME/ls_usgs_l1_prepare.py {} --output $HOME/yamls_l1/TARGET --no-checksum --date 1/1/1999"
