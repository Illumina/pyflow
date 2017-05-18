#!/usr/bin/env python
"""
A dummy worker used to illustrate a pyflow 'launch-tasks-until-condition' workflow.

This worker wait for a random length of time, then writes a random number to the file
specified in arg1.
"""

import sys

if len(sys.argv) != 2 :
    print "usage: $0 outputFile"
    sys.exit(2)

outputFile=sys.argv[1]

import time
from random import randint
sleepTime=randint(1,60)
time.sleep(sleepTime)

reportVal=randint(1,20)

ofp=open(outputFile,"wb")
ofp.write("%i\n" % (reportVal))
ofp.close()

