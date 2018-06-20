#HOME=/g/data1b/da82/AODH/USGS/L1/Landsat/C1
#DATA=/g/data1b/da82/AODH/USGS/L1/Landsat/C1
HOME=/g/data2/v10/AGDCv2/datacube-ingestion/indexed-products/ledaps_lasrc/opendatacubepipelines.ledapslasrc
DATA=/g/data2/v10/AGDCv2/datacube-ingestion/indexed-products/ledaps_lasrc/opendatacubepipelines.ledapslasrc/test_data

for i in `ls -1 $DATA`; do if [[ $i != *":"* ]]; then  mkdir -p $HOME/yamls_test/$i ; fi; done
for i in `ls $HOME/yamls_test/`; do cp ledaps_lasrc_prepare_l1.sh $HOME/qsub_scripts_l1/$i.qsub; sed -i -e "s/TARGET/$i/g" "$HOME/qsub_scripts_l1/$i.qsub"; echo 'qsub $HOME/qsub_scripts_l1/'$i'.qsub'; done
