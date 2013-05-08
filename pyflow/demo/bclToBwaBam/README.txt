
This demo shows the use of pyflow on a production-scale problem.

The "configBclToBwaBam.py" script here will take one or more bcl
basecalls directories, run them through CASAVA 1.8 bcl conversion and
align/sort/merge/markdup each sample into a single BAM file. A list of
sample names may be given to restrict the analysis post bcl
conversion.

Help for the configuration script is available by typing
"./configBclToBwaBam.py -h". To run, the script requires at minimum a
bcl basecalls directory and a BWA index genome fasta file.

This directory contains a configuration file
"configBclToBwaBam.py.ini" which contains paths for bwa, samtools,
Picard and CASAVA. You may need to change these to reflect the
installed location at your site before running

If on the sd-isilon, the file "example_configuration.bash" will call
"configBclToBwaBam.py" with a pointer to a subsampled bcl directory to
quickly demonstate the use of this script on real data.

Note that once all arguments are provided and the configuration script
completes, a run script will be generated in the output directory
which can be used to actually execute the workflow, allowing for
local/sge and total job limit specification.

