HOME=/g/data/v10/AGDCv2/indexed_datasets/ledaps_lasrc/opendatacubepipelines.ledapslasrc 
DATA=/g/data/dz56/ARD_interoperability/L2

for i in `ls -1 $DATA/unzip`; do if [[ $i != *":"* ]]; then  mkdir -p $DATA/yamls/$i ; fi; done
for i in `ls $DATA/yamls/`; do cp ledaps_lasrc_prepare.sh $HOME/qsub_scripts/$i.qsub; sed -i -e "s/TARGET/$i/g" "$HOME/qsub_scripts/$i.qsub"; echo 'qsub $HOME/qsub_scripts/'$i'.qsub'; done
