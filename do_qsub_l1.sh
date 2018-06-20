HOME=/g/data2/v10/AGDCv2/datacube-ingestion/indexed-products/ledaps_lasrc/opendatacubepipelines.ledapslasrc

for i in `find $HOME/qsub_scripts_l1 -name "*.qsub"`; do qsub $i; done

