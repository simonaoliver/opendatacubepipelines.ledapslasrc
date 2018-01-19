for i in `find /g/data/v10/AGDCv2/indexed_datasets/ledaps_lasrc/opendatacubepipelines.ledapslasrc/qsub_scripts_l1 -name "*.qsub"`; do qsub $i; done

