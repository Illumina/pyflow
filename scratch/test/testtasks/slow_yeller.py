#!/usr/bin/env python

import os,sys,time
import datetime

if len(sys.argv) != 2 :
    print "usage $0 arg"
    sys.exit(1)

arg=sys.argv[1]

pid=os.getpid()

sys.stdout.write("pid: %s arg: %s starting yell\n" % (str(pid),arg))

for i in xrange(100):
    td=datetime.datetime.utcnow().isoformat()
    msg="Yeller %s yellin %i" % (str(pid),i)
    sys.stdout.write(msg+" stdout "+td+"\n")
    sys.stderr.write(msg+" stderr "+td+"\n")
    time.sleep(1)

sys.stdout.write("pid: %s arg: %s ending yell\n" % (str(pid),arg))

