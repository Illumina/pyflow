#!/usr/bin/env bash

set -o xtrace

#
# executes the configure script for a small bcl directory -- note that
# the tile mask is required for this bcl directory because it has been
# extensively subsampled for testing purposes
#

./configBclToBwaBam.py \
--bclBasecallsDir /home/csaunders/proj/bwa_workflow_hashout/create_small_lane/small_lane/111119_SN192_0307_BD0FNCACXX_Genentech/Data/Intensities/BaseCalls \
--bclTilePattern "s_8_[02468][0-9][0-9]1" \
--bclBasecallsDir /home/csaunders/proj/bwa_workflow_hashout/create_small_lane/small_lane/111119_SN192_0307_BD0FNCACXX_Genentech/Data/Intensities/BaseCalls \
--bclTilePattern "s_8_[13579][0-9][0-9]1" \
--genomeFasta /illumina/scratch/iGenomes/Homo_sapiens/UCSC/hg19/Sequence/BWAIndex/genome.fa \
--sampleName "lane8"

