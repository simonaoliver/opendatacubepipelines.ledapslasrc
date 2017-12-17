#!/bin/bash
#
#PBS -P v10
#PBS -q normal
#PBS -l walltime=48:00:00,ncpus=1,mem=8GB
#PBS -l wd
#PBS -me

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles/
module load gaip/dev-sen2redo
#module load agdc-py3-prod/1.5.1
module load parallel

HOME=/g/data/v10/AGDCv2/indexed_datasets/ledaps_lasrc/opendatacubepipelines.ledapslasrc
DATA=/g/data/v10/projects/ARD_interoperability/L2

python $HOME/ls_usgs_l2_prepare.py $DATA/TARGET --output $HOME/yamls/TARGET --no-checksum --date 1/1/1999
