#!/usr/bin/env bash

thisdir=$(dirname $0)

PYTHONPATH=$thisdir/../../src epydoc pyflow.WorkflowRunner --no-private -o WorkflowRunner_API_html_doc

