#!/usr/bin/env bash

thisdir=$(dirname $0)

epydoc $thisdir/../../src/*.py -o pyflow_developer_html_doc -v --graph all

