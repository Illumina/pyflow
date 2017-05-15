#!/usr/bin/env bash
#
# run all code health scripts in one shot... this is a good pre-release step
#

set -o errexit
set -o nounset

this_dir=$(dirname $0)

script_dir=$this_dir/source_check_and_format


$script_dir/source_header_scripts/reheader_all_source.bash

$script_dir/delete_trailing_wspace.bash -imeanit

echo
echo "Source update complete."
echo

