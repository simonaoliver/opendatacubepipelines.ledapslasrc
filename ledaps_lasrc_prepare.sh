#!/bin/bash
#
#PBS -P v10
#PBS -q normal
#PBS -l walltime=2:00:00,ncpus=8,mem=64GB
#PBS -l wd
#PBS -me

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles/
module load gaip/dev-sen2redo
#module load agdc-py3-prod/1.5.1
module load parallel

find /g/data/v10/projects/ARD_interoperability/L2/TARGET -name *tar.gz | parallel --jobs 8 "python ls_usgs_l2_prepare.py {} --output /g/data/v10/AGDCv2/indexed_datasets/ledaps_lasrc/opendatacubepipelines.ledapslasrc/yamls/TARGET --no-checksum --date 1/1/1999"
