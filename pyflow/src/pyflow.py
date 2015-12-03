#!/usr/bin/env python
#
# pyFlow - a lightweight parallel task engine
#
# Copyright (c) 2012-2015 Illumina, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

"""
pyflow -- a lightweight parallel task engine
"""

__author__ = 'Christopher Saunders'


import copy
import datetime
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import traceback

from pyflowConfig import siteConfig


moduleDir = os.path.abspath(os.path.dirname(__file__))


# minimum python version
#
pyver = sys.version_info
if pyver[0] != 2 or (pyver[0] == 2 and pyver[1] < 4) :
    raise Exception("pyflow module has only been tested for python versions [2.4,3.0)")

# problem python versions:
#
# Internal interpreter deadlock issue in python 2.7.2:
# http://bugs.python.org/issue13817
# ..is so bad that pyflow can partially, but not completely, work around it -- so issue a warning for this case.
if pyver[0] == 2 and pyver[1] == 7 and pyver[2] == 2 :
    raise Exception("Python interpreter errors in python 2.7.2 may cause a pyflow workflow hang or crash. Please use a different python version.")


# The line below is a workaround for a python 2.4/2.5 bug in
# the subprocess module.
#
# Bug is described here: http://bugs.python.org/issue1731717
# Workaround is described here: http://bugs.python.org/issue1236
#
subprocess._cleanup = lambda: None


# In python 2.5 or greater, we can lower the per-thread stack size to
# improve memory consumption when a very large number of jobs are
# run. Below it is lowered to 256Kb (compare to linux default of
# 8Mb).
#
try:
    threading.stack_size(min(256 * 1024, threading.stack_size))
except AttributeError:
    # Assuming this means python version < 2.5
    pass


class GlobalSync :
    """
    Control total memory usage in non-local run modes by
    limiting the number of simultaneous subprocess calls

    Note that in practice this only controls the total number
    of qsub/qstat calls in SGE mode
    """
    maxSubprocess = 2
    subprocessControl = threading.Semaphore(maxSubprocess)



def getPythonVersion() :
    python_version = sys.version_info
    return ".".join([str(i) for i in python_version])

pythonVersion = getPythonVersion()


# Get pyflow version number
#

def getPyflowVersion() :
    # this will be automatically macro-ed in for pyflow releases:
    pyflowAutoVersion = None

    # Get version number in regular release code:
    if pyflowAutoVersion is not None : return pyflowAutoVersion

    # Get version number during dev:
    try :
        proc = subprocess.Popen(["git", "describe"], stdout=subprocess.PIPE, stderr=open(os.devnull, "w"), cwd=moduleDir, shell=False)
        (stdout, _stderr) = proc.communicate()
        retval = proc.wait()
        stdoutList = stdout.split("\n")[:-1]
        if (retval == 0) and (len(stdoutList) == 1) : return stdoutList[0]
    except OSError:
        # no git installed
        pass

    return "unknown"


__version__ = getPyflowVersion()


# portability functions:
#

def _isWindows() :
    import platform
    return (platform.system().find("Windows") > -1)

class GlobalConstants :
    isWindows=_isWindows()


def isWindows() :
    return GlobalConstants.isWindows




def forceRename(src,dst) :
    """
    dst is only overwritten in a single atomic operation on *nix
    on windows, we can't have atomic rename, but we can recreate the behavior otherwise
    """
    if isWindows() :
        if os.path.exists(dst) :
            os.remove(dst)

    maxTrials=5
    for trial in range(maxTrials) :
        try :
            os.rename(src,dst)
            return
        except OSError :
            if (trial+1) >= maxTrials : raise
            time.sleep(5)



def cleanEnv() :
    """
    clear bash functions out of the env

    without this change the shellshock security update causes pyflow SGE jobs to
    fail with the behavior of current (201512) versions of SGE qsub
    """

    ekeys = os.environ.keys()
    for key in ekeys :
        if key.endswith("()") :
            del os.environ[key]


# utility values and functions:
#

def ensureDir(d):
    """
    make directory if it doesn't already exist, raise exception if
    something else is in the way:
    """
    if os.path.exists(d):
        if not os.path.isdir(d) :
            raise Exception("Can't create directory: %s" % (d))
    else :
        os.makedirs(d)


#
# time functions -- note there's an additional copy in the pyflow wrapper script:
#
# all times in pyflow are utc (never local) and printed to iso8601
#
def timeStampToTimeStr(ts) :
    """
    converts time.time() output to timenow() string
    """
    return datetime.datetime.utcfromtimestamp(ts).isoformat()

def timeStrNow():
    return timeStampToTimeStr(time.time())

def timeStrToTimeStamp(ts):
    import calendar
    d = datetime.datetime(*map(int, re.split(r'[^\d]', ts)[:-1]))
    return calendar.timegm(d.timetuple())



def isInt(x) :
    return isinstance(x, (int, long))

def isString(x):
    return isinstance(x, basestring)


def isIterable(x):
    return (getattr(x, '__iter__', False) != False)


def lister(x):
    """
    Convert input into a list, whether it's already iterable or
    not. Make an exception for individual strings to be returned
    as a list of one string, instead of being chopped into letters
    Also, convert None type to empty list:
    """
    # special handling in case a single string is given:
    if x is None : return []
    if (isString(x) or (not isIterable(x))) : return [x]
    return list(x)



def setzer(x) :
    """
    convert user input into a set, handling the pathological case
    that you have been handed a single string, and you don't want
    a set of letters:
    """
    return set(lister(x))



class LogState :
    """
    A simple logging enum
    """
    INFO = 1
    WARNING = 2
    ERROR = 3

    @classmethod
    def toString(cls,logState) :
        if logState == cls.INFO : return "INFO"
        if logState == cls.WARNING : return "WARNING"
        if logState == cls.ERROR : return "ERROR"

        raise Exception("Unknown log state: " + str(logState))


# allow fsync to be globally turned off
class LogGlobals :
    isFsync = True


def hardFlush(ofp):
    ofp.flush()
    if ofp.isatty() : return
    # fsync call has been reported to consistently fail in some contexts (rsh?)
    # so allow OSError
    if not LogGlobals.isFsync : return
    try :
        os.fsync(ofp.fileno())
    except OSError:
        LogGlobals.isFsync = False



def log(ofpList, msgList, linePrefix=None):
    """
    General logging function.

    @param ofpList: A container of file objects to write to

    @param msgList: A container of (or a single) multi-line log message
               string. Final newlines are not required

    @param linePrefix: A prefix to add before every line. This will come
                  *after* the log function's own '[time] [hostname]'
                  prefix.

    @return: Returns a boolean tuple of size ofpList indicating the success of
      writing to each file object
    """
    msgList = lister(msgList)
    ofpList = setzer(ofpList)
    retval = [True] * len(ofpList)
    for msg in msgList :
        # strip final trailing newline if it exists:
        if (len(msg) > 0) and (msg[-1] == "\n") : msg = msg[:-1]
        linePrefixOut = "[%s] [%s]" % (timeStrNow(), siteConfig.getHostName())
        if linePrefix is not None : linePrefixOut += " " + linePrefix
        # split message into prefixable lines:
        for i, ofp in enumerate(ofpList):
            # skip io streams which have failed before:
            if not retval[i] : continue
            try :
                for line in msg.split("\n") :
                    ofp.write("%s %s\n" % (linePrefixOut, line))
                hardFlush(ofp)
            except IOError:
                retval[i] = False
    return retval



def getThreadName():
    return threading.currentThread().getName()

def isMainThread() :
    return (getThreadName == "MainThread")


class StrFileObject(object) :
    """
    fakes a filehandle for library functions which write to a stream,
    and captures output in a string
    """
    def __init__(self) :
        self.str = ""

    def write(self, string) :
        self.str += string

    def __str__(self) :
        return self.str


def getTracebackStr() :
    return traceback.format_exc()


def getExceptionMsg() :

    msg = ("Unhandled Exception in %s\n" % (getThreadName())) + getTracebackStr()
    if msg[-1] == "\n" : msg = msg[:-1]
    return msg.split("\n")


def cmdline() :
    return " ".join(sys.argv)



def msgListToMsg(msgList):
    """
    convert string or list of strings into a single string message
    """
    msg = ""
    isFirst=True
    for chunk in lister(msgList) :
        if isFirst :
            isFirst = False
        else :
            msg += "\n"
        if ((len(chunk)>0) and (chunk[-1] == '\n')) :
            chunk = chunk[:-1]
        msg += chunk

    return msg



emailRegex = re.compile(r"(?:^|\s)[-a-z0-9_.]+@(?:[-a-z0-9]+\.)+[a-z]{2,6}(?:\s|$)", re.IGNORECASE)

def verifyEmailAddy(x) :
    return (emailRegex.match(x) is not None)


def isLocalSmtp() :
    """
    return true if a local smtp server is available
    """
    import smtplib
    try :
        s = smtplib.SMTP('localhost')
    except :
        return False
    return True


def sendEmail(mailTo, mailFrom, subject, msgList) :
    import smtplib
    # this is the way to import MIMEText in py 2.4:
    from email.MIMEText import MIMEText

    # format message list into a single string:
    msg = msgListToMsg(msgList)

    mailTo = setzer(mailTo)

    msg = MIMEText(msg)
    msg["Subject"] = subject
    msg["From"] = mailFrom
    msg["To"] = ", ".join(mailTo)

    s = smtplib.SMTP('localhost')
    s.sendmail(mailFrom, list(mailTo), msg.as_string())
    s.quit()


def boolToStr(b) :
    return str(int(b))


def argToBool(x) :
    """
    convert argument of unknown type to a bool:
    """
    class FalseStrings :
        val = ("", "0", "false", "f", "no", "n", "off")

    if isinstance(x, basestring) :
        return (x.lower() not in FalseStrings.val)
    return bool(x)


def hashObjectValue(obj) :
    """
    This function hashes objects values -- the hash will be the
    same for two objects containing the same methods and data, so
    it corresponds to 'A==B' and *not* 'A is B'.
    """
    import pickle
    import hashlib
    hashlib.md5(pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)).hexdigest()


namespaceSep = "+"


def namespaceJoin(a, b) :
    """
    join two strings with a separator only if a exists
    """
    if a == "" : return b
    elif b == "" : return a
    return a + namespaceSep + b


def namespaceLabel(namespace) :
    """
    provide a consistent naming scheme to users for embedded workflows
    """
    if namespace == "" :
        return "master workflow"
    else :
        return "sub-workflow '%s'" % (namespace)



class ExpWaiter(object) :
    """
    Convenience object to setup exponentially increasing wait/polling times
    """
    def __init__(self, startSec, factor, maxSec, event = None) :
        """
        optionally allow an event to interrupt wait cycle
        """
        assert (startSec > 0.)
        assert (factor > 1.)
        assert (maxSec >= startSec)
        self.startSec = startSec
        self.factor = factor
        self.maxSec = maxSec
        self.event = event

        self.sec = self.startSec
        self.isMax = False

    def reset(self) :
        self.sec = self.startSec

    def wait(self) :
        if self.event is None :
            time.sleep(self.sec)
        else :
            self.event.wait(self.sec)
        if self.isMax : return
        self.sec = min(self.sec * self.factor, self.maxSec)
        self.isMax = (self.sec == self.maxSec)
        assert self.sec <= self.maxSec



def lockMethod(f):
    """
    method decorator acquires/releases object's lock
    """

    def wrapped(self, *args, **kw):
        if not hasattr(self,"lock") :
            self.lock = threading.RLock()

        self.lock.acquire()
        try:
            return f(self, *args, **kw)
        finally:
            self.lock.release()
    return wrapped



class Bunch:
    """
    generic struct with named argument constructor
    """
    def __init__(self, **kwds):
        self.__dict__.update(kwds)



def stackDump(dumpfp):
    """
    adapted from haridsv @ stackoverflow:
    """

    athreads = threading.enumerate()
    tnames = [(th.getName()) for th in athreads]

    frames = None
    try:
        frames = sys._current_frames()
    except AttributeError:
        # python version < 2.5
        pass

    id2name = {}
    try:
        id2name = dict([(th.ident, th.getName()) for th in athreads])
    except AttributeError :
        # python version < 2.6
        pass

    if (frames is None) or (len(tnames) > 50) :
        dumpfp.write("ActiveThreadCount: %i\n" % (len(tnames)))
        dumpfp.write("KnownActiveThreadNames:\n")
        for name in tnames : dumpfp.write("  %s\n" % (name))
        dumpfp.write("\n")
        return

    dumpfp.write("ActiveThreadCount: %i\n" % (len(frames)))
    dumpfp.write("KnownActiveThreadNames:\n")
    for name in tnames : dumpfp.write("  %s\n" % (name))
    dumpfp.write("\n")

    for tid, stack in frames.items():
        dumpfp.write("Thread: %d %s\n" % (tid, id2name.get(tid, "NAME_UNKNOWN")))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            dumpfp.write('File: "%s", line %d, in %s\n' % (filename, lineno, name))
            if line is not None:
                dumpfp.write("  %s\n" % (line.strip()))
        dumpfp.write("\n")
    dumpfp.write("\n")




#######################################################################
#
# these functions are written out to a utility script which allows users
# to make a dot graph from their current state directory output. We
# keep it in pyflow as working code so that pyflow can call sections of it.
#

def taskStateHeader() :
    return "#taskLabel\ttaskNamespace\trunState\terrorCode\trunStateUpdateTime\n"


def taskStateParser(stateFile) :
    class Constants :
        nStateCols = 5

    for line in open(stateFile) :
        if len(line) and line[0] == "#" : continue
        line = line.strip()
        w = line.split("\t")
        if len(w) != Constants.nStateCols :
            raise Exception("Unexpected format in taskStateFile: '%s' line: '%s'" % (stateFile, line))
        yield [x.strip() for x in w]


def taskInfoHeader() :
    return "#%s\n" % ("\t".join(("taskLabel", "taskNamespace", "taskType", "nCores", "memMb", "priority", "isForceLocal", "dependencies", "cwd", "command")))


def taskInfoParser(infoFile) :
    class Constants :
        nInfoCols = 10

    for line in open(infoFile) :
        if len(line) and line[0] == "#" : continue
        line = line.lstrip()
        w = line.split("\t", (Constants.nInfoCols - 1))
        if len(w) != Constants.nInfoCols :
            raise Exception("Unexpected format in taskInfoFile: '%s' line: '%s'" % (infoFile, line))
        yield [x.strip() for x in w]


def getTaskInfoDepSet(s) :
    # reconstruct dependencies allowing for extraneous whitespace in the file:
    s = s.strip()
    if s == "" : return []
    return set([d.strip() for d in s.split(",")])



class TaskNodeConstants(object) :

    validRunstates = ("complete", "running", "queued", "waiting", "error")



class DotConfig(object) :
    """
    A static container of configuration data for dot graph output
    """

    runstateDotColor = {"waiting" : "grey",
                        "running" : "green",
                        "queued" : "yellow",
                        "error" : "red",
                        "complete" : "blue" }

    runstateDotStyle = {"waiting" : "dashed",
                        "running" : None,
                        "queued" : None,
                        "error" : "bold",
                        "complete" : None }

    @staticmethod
    def getRunstateDotAttrib(runstate) :
        color = DotConfig.runstateDotColor[runstate]
        style = DotConfig.runstateDotStyle[runstate]
        attrib = ""
        if color is not None : attrib += " color=%s" % (color)
        if style is not None : attrib += " style=%s" % (style)
        return attrib

    @staticmethod
    def getTypeDotAttrib(nodeType) :
        attrib = ""
        if nodeType == "workflow" :
            attrib += " shape=rect style=rounded"
        return attrib

    @staticmethod
    def getDotLegend() :
        string = '{ rank = source; Legend [shape=none, margin=0, label=<\n'
        string += '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">\n'
        string += '<TR><TD COLSPAN="2">Legend</TD></TR>\n'
        for state in TaskNodeConstants.validRunstates :
            color = DotConfig.runstateDotColor[state]
            string += '<TR> <TD>%s</TD> <TD BGCOLOR="%s"></TD> </TR>\n' % (state, color)
        string += '</TABLE>>];}\n'
        return string



def writeDotGraph(taskInfoFile, taskStateFile, workflowClassName) :
    """
    write out the current graph state in dot format
    """

    addOrder = []
    taskInfo = {}
    headNodes = set()
    tailNodes = set()

    # read info file:
    for (label, namespace, ptype, _nCores, _memMb, _priority, _isForceLocal, depStr, _cwdStr, _command) in taskInfoParser(taskInfoFile) :
        tid = (namespace, label)
        addOrder.append(tid)
        taskInfo[tid] = Bunch(ptype=ptype,
                              parentLabels=getTaskInfoDepSet(depStr))
        if len(taskInfo[tid].parentLabels) == 0 : headNodes.add(tid)
        tailNodes.add(tid)
        for plabel in taskInfo[tid].parentLabels :
            ptid = (namespace, plabel)
            if ptid in tailNodes : tailNodes.remove(ptid)

    for (label, namespace, runState, _errorCode, _time) in taskStateParser(taskStateFile) :
        tid = (namespace, label)
        taskInfo[tid].runState = runState

    dotFp = sys.stdout
    dotFp.write("// Task graph from pyflow object '%s'\n" % (workflowClassName))
    dotFp.write("// Process command: '%s'\n" % (cmdline()))
    dotFp.write("// Process working dir: '%s'\n" % (os.getcwd()))
    dotFp.write("// Graph capture time: %s\n" % (timeStrNow()))
    dotFp.write("\n")
    dotFp.write("digraph %s {\n" % (workflowClassName + "Graph"))
    dotFp.write("\tcompound=true;\nrankdir=LR;\nnode[fontsize=10];\n")
    labelToSym = {}
    namespaceGraph = {}
    for (i, (namespace, label)) in enumerate(addOrder) :
        tid = (namespace, label)
        if namespace not in namespaceGraph :
            namespaceGraph[namespace] = ""
        sym = "n%i" % i
        labelToSym[tid] = sym
        attrib1 = DotConfig.getRunstateDotAttrib(taskInfo[tid].runState)
        attrib2 = DotConfig.getTypeDotAttrib(taskInfo[tid].ptype)
        namespaceGraph[namespace] += "\t\t%s [label=\"%s\"%s%s];\n" % (sym, label, attrib1, attrib2)

    for (namespace, label) in addOrder :
        tid = (namespace, label)
        sym = labelToSym[tid]
        for plabel in taskInfo[tid].parentLabels :
            ptid = (namespace, plabel)
            namespaceGraph[namespace] += ("\t\t%s -> %s;\n" % (labelToSym[ptid], sym))

    for (i, ns) in enumerate(namespaceGraph.keys()) :
        isNs = ((ns is not None) and (ns != ""))
        dotFp.write("\tsubgraph cluster_sg%i {\n" % (i))
        if isNs :
            dotFp.write("\t\tlabel = \"%s\";\n" % (ns))
        else :
            dotFp.write("\t\tlabel = \"%s\";\n" % (workflowClassName))
        dotFp.write(namespaceGraph[ns])
        dotFp.write("\t\tbegin%i [label=\"begin\" shape=diamond];\n" % (i))
        dotFp.write("\t\tend%i [label=\"end\" shape=diamond];\n" % (i))
        for (namespace, label) in headNodes :
            if namespace != ns : continue
            sym = labelToSym[(namespace, label)]
            dotFp.write("\t\tbegin%i -> %s;\n" % (i, sym))
        for (namespace, label) in tailNodes :
            if namespace != ns : continue
            sym = labelToSym[(namespace, label)]
            dotFp.write("\t\t%s -> end%i;\n" % (sym, i))
        dotFp.write("\t}\n")
        if ns in labelToSym :
            dotFp.write("\t%s -> begin%i [style=dotted];\n" % (labelToSym[ns], i))
            # in LR orientation this will make the graph look messy:
            # dotFp.write("\tend%i -> %s [style=invis];\n" % (i,labelToSym[ns]))

    dotFp.write(DotConfig.getDotLegend())
    dotFp.write("}\n")
    hardFlush(dotFp)



def writeDotScript(taskDotScriptFile,
                   taskInfoFileName, taskStateFileName,
                   workflowClassName) :
    """
    write dot task graph creation script
    """
    import inspect

    dsfp = os.fdopen(os.open(taskDotScriptFile, os.O_WRONLY | os.O_CREAT, 0755), 'w')

    dsfp.write("""#!/usr/bin/env python
#
# This is a script to create a dot graph from pyflow state files.
# Usage: $script >| task_graph.dot
#
# Note that script assumes the default pyflow state files are in the script directory.
#
# This file was autogenerated by process: '%s'
# ...from working directory: '%s'
#

import datetime,os,sys,time

scriptDir=os.path.abspath(os.path.dirname(__file__))
""" % (os.getcwd(), cmdline()))

    for dobj in (timeStampToTimeStr, timeStrNow, cmdline, Bunch, LogGlobals, hardFlush, TaskNodeConstants, DotConfig, taskStateParser, taskInfoParser, getTaskInfoDepSet, writeDotGraph) :
        dsfp.write("\n\n")
        dsfp.write(inspect.getsource(dobj))

    dsfp.write("""

if __name__ == '__main__' :
    writeDotGraph(os.path.join(scriptDir,'%s'),os.path.join(scriptDir,'%s'),'%s')

""" % (taskInfoFileName, taskStateFileName, workflowClassName))



################################################################
#
# workflowRunner Helper Classes:
#
#


class Command(object) :
    """
    Commands can be presented as strings or argument lists (or none)
    """

    def __init__(self, cmd, cwd, env=None) :
        # 1: sanitize/error-check cmd
        if ((cmd is None)  or
            (cmd == "") or
            (isIterable(cmd) and len(cmd) == 0)) :
            self.cmd = None
            self.type = "none"
        elif isString(cmd) :
            self.cmd = Command.cleanStr(cmd)
            self.type = "str"
        elif isIterable(cmd) :
            self.cmd = []
            for i, s in enumerate(cmd):
                if not (isString(s) or isInt(s)):
                    raise Exception("Argument: '%s' from position %i in argument list command is not a string or integer. Full command: '%s'" %
                                    (str(s), (i + 1), " ".join([str(s) for s in cmd])))
                self.cmd.append(Command.cleanStr(s))
            self.type = "list"
        else :
            raise Exception("Invalid task command: '%s'" % (str(cmd)))

        # 2: sanitize cwd
        self.cwd = ""
        if cwd is not None and cwd != "" :
            self.cwd = os.path.abspath(cwd)
            if os.path.exists(self.cwd) and not os.path.isdir(self.cwd) :
                raise Exception("Cwd argument is not a directory: '%s', provided for command '%s'" % (cwd, str(cmd)))

        # copy env:
        self.env = env

    def __repr__(self) :
        if self.cmd is None : return ""
        if self.type == "str" : return self.cmd
        return " ".join(self.cmd)

    @staticmethod
    def cleanStr(s) :
        if isInt(s) : s = str(s)
        if "\n" in s : raise Exception("Task command/argument contains newline characters: '%s'" % (s))
        return s.strip()



class StoppableThread(threading.Thread):
    """
    Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition.

    Note that this is a very new thread base class for pyflow, and most
    threads do not (yet) check their stopped status.

    """

    _stopAll = threading.Event()

    def __init__(self, *args, **kw):
        threading.Thread.__init__(self, *args, **kw)
        self._stop = threading.Event()

    def stop(self):
        "thread specific stop method, may be overridden to add async thread-specific kill behavior"
        self._stop.set()

    @staticmethod
    def stopAll():
        "quick global stop signal for threads that happen to poll stopped() very soon after event"
        StoppableThread._stopAll.set()

    def stopped(self):
        return (StoppableThread._stopAll.isSet() or self._stop.isSet())



def getSGEJobsDefault() :
    if ((siteConfig.maxSGEJobs is not None) and
        (siteConfig.maxSGEJobs != "") and
        (siteConfig.maxSGEJobs != "unlimited")) :
        return int(siteConfig.maxSGEJobs)
    return "unlimited"



class ModeInfo(object) :
    """
    Stores default values associated with each runmode: local,sge,...
    """
    def __init__(self, defaultCores, defaultMemMbPerCore, defaultIsRetry) :
        self.defaultCores = defaultCores
        self.defaultMemMbPerCore = defaultMemMbPerCore
        self.defaultIsRetry = defaultIsRetry



class RunMode(object):

    data = { "local" : ModeInfo(defaultCores=1,
                                defaultMemMbPerCore=siteConfig.defaultHostMemMbPerCore,
                                defaultIsRetry=False),
             "sge"   : ModeInfo(defaultCores=getSGEJobsDefault(),
                                defaultMemMbPerCore="unlimited",
                                defaultIsRetry=True) }



class RetryParam(object) :
    """
    parameters pertaining to task retry behavior
    """
    allowed_modes = [ "nonlocal" , "all" ]

    def __init__(self, run_mode, retry_max, wait, window, retry_mode) :
        if retry_mode not in self.allowed_modes :
            raise Exception("Invalid retry mode parameter '%s'. Accepted retry modes are {%s}." \
                            % (retry_mode, ",".join(self.allowed_modes)))

        self._retry_max = retry_max
        self.wait = wait
        self.window = window
        self._retry_mode = retry_mode
        self._run_mode = run_mode

        self._finalize()
        self.validate()


    def _finalize(self) :
        """
        decide whether to turn retry off based on retry and run modes:
        """
        if (self._retry_mode == "nonlocal") and \
                (not RunMode.data[self._run_mode].defaultIsRetry) :
            self.max = 0
        else :
            self.max = int(self._retry_max)


    def validate(self):
        """
        check that the public parameters are valid
        """
        def nonNegParamCheck(val, valLabel) :
            if val < 0 : raise Exception("Parameter %s must be non-negative" % valLabel)

        nonNegParamCheck(self.max, "retryMax")
        nonNegParamCheck(self.wait, "retryWait")
        nonNegParamCheck(self.window, "retryWindow")


    def getTaskCopy(self,retry_max, wait, window, retry_mode):
        """
        return a deepcopy of the class customized for each individual task for
        any retry parameters which are not None
        """
        taskself = copy.deepcopy(self)

        if retry_max is not None:
            taskself._retry_max = retry_max
        if wait is not None:
            taskself.wait = wait
        if window is not None:
            taskself.window = window
        if retry_mode is not None :
            taskself._retry_mode = retry_mode

        taskself._finalize()
        taskself.validate()
        return taskself


class RunningTaskStatus(object) :
    """
    simple object allowing remote task threads to communicate their
    status back to the TaskManager
    """
    def __init__(self,isFinishedEvent) :
        self.isFinishedEvent = isFinishedEvent
        self.isComplete = threading.Event()
        self.errorCode = 0

        # errorMessage is filled in by sub-workflow
        # and command-line tasks.
        #
        # Sub-workflows use this to convey whether they have
        # failed (1) because of failures of their own tasks or (2)
        # because of an exception in the sub-workflow code, in which
        # case the exception message and stacktrace are provided.
        #
        # command tasks use this to report the stderr tail of a failing
        # task
        #
        self.errorMessage = ""

        # only used by sub-workflows to indicate that all tasks have been specified
        self.isSpecificationComplete = threading.Event()


class BaseTaskRunner(StoppableThread) :
    """
    Each individual command-task or sub workflow task
    is run on its own thread using a class inherited from
    BaseTaskRunner
    """

    def __init__(self, runStatus, taskStr, sharedFlowLog, setRunstate) :
        StoppableThread.__init__(self)
        self.setDaemon(True)
        self.taskStr = taskStr
        self.setName("TaskRunner-Thread-%s" % (taskStr))
        self.runStatus = runStatus
        self._sharedFlowLog = sharedFlowLog
        self.lock = threading.RLock()

        # allows taskRunner to update between queued and running status:
        self._setRunstate = setRunstate

        # this is moved into the ctor now, so that a race condition that would double-launch a task
        # is now not possible (however unlikely it was before):
        self.setInitialRunstate()


    def run(self) :
        """
        BaseTaskRunner's run() method ensures that we can
        capture exceptions which might occur in this thread.
        Do not override this method -- instead define the core
        logic for the task run operation in '_run()'

        Note that for sub-workflow tasks we're interpreting raw
        client python code on this thread, so exceptions are
        *very likely* here -- this is not a corner case.
        """
        retval = 1
        retmsg = ""
        try:
            (retval, retmsg) = self._run()
        except WorkflowRunner._AbortWorkflowException :
            # This indicates an intended workflow interruption.
            # send a retval of 1 but not an error message
            pass
        except:
            retmsg = getExceptionMsg()
        self.runStatus.errorCode = retval
        self.runStatus.errorMessage = retmsg
        # this indicates that this specific task has finished:
        self.runStatus.isComplete.set()
        # this indicates that *any* task has just finished, so
        # taskmanager can stop polling and immediately sweep
        self.runStatus.isFinishedEvent.set()
        return retval

    def setRunstate(self, *args, **kw) :
        if self._setRunstate is None : return
        self._setRunstate(*args, **kw)

    def setInitialRunstate(self) :
        self.setRunstate("running")

    def flowLog(self, msg, logState) :
        linePrefixOut = "[TaskRunner:%s]" % (self.taskStr)
        self._sharedFlowLog(msg, linePrefix=linePrefixOut, logState=logState)

    def infoLog(self, msg) :
        self.flowLog(msg, logState=LogState.INFO)

    def warningLog(self, msg) :
        self.flowLog(msg, logState=LogState.WARNING)

    def errorLog(self, msg) :
        self.flowLog(msg, logState=LogState.ERROR)



class WorkflowTaskRunner(BaseTaskRunner) :
    """
    Manages a sub-workflow task
    """

    def __init__(self, runStatus, taskStr, workflow, sharedFlowLog, setRunstate) :
        BaseTaskRunner.__init__(self, runStatus, taskStr, sharedFlowLog, setRunstate)
        self.workflow = workflow

    def _run(self) :
        namespace = self.workflow._getNamespace()
        nsLabel = namespaceLabel(namespace)
        self.infoLog("Starting task specification for %s" % (nsLabel))
        self.workflow._setRunning(True)
        self.workflow.workflow()
        self.workflow._setRunning(False)
        self.runStatus.isSpecificationComplete.set()
        self.infoLog("Finished task specification for %s, waiting for task completion" % (nsLabel))
        retval = self.workflow._waitForTasksCore(namespace, isVerbose=False)
        retmsg = ""
        return (retval, retmsg)


class CommandTaskRunner(BaseTaskRunner) :
    """
    Parent to local and SGE TaskRunner specializations for command tasks
    """

    taskWrapper = os.path.join(moduleDir, "pyflowTaskWrapper.py")

    def __init__(self, runStatus, runid, taskStr, cmd, nCores, memMb, retry, isDryRun,
                 outFile, errFile, tmpDir, schedulerArgList,
                 sharedFlowLog, setRunstate) :
        """
        @param outFile: stdout file
        @param errFile: stderr file
        @param tmpDir: location to write files containing output from
                  the task wrapper script (and not the wrapped task)
        """
        import pickle

        BaseTaskRunner.__init__(self, runStatus, taskStr, sharedFlowLog, setRunstate)

        self.cmd = cmd
        self.nCores = nCores
        self.memMb = memMb
        self.retry = retry
        self.isDryRun = isDryRun
        self.outFile = outFile
        self.errFile = errFile
        self.tmpDir = tmpDir
        self.schedulerArgList = schedulerArgList
        if not os.path.isfile(self.taskWrapper) :
            raise Exception("Can't find task wrapper script: %s" % self.taskWrapper)

        ensureDir(self.tmpDir)
        self.wrapFile = os.path.join(self.tmpDir, "pyflowTaskWrapper.signal.txt")

        # setup all the data to be passed to the taskWrapper and put this in argFile:
        taskInfo = { 'nCores' : nCores,
                   'outFile' : outFile, 'errFile' : errFile,
                   'cwd' : cmd.cwd, 'env' : cmd.env,
                   'cmd' : cmd.cmd, 'isShellCmd' : (cmd.type == "str") }

        argFile = os.path.join(self.tmpDir, "taskWrapperParameters.pickle")
        pickle.dump(taskInfo, open(argFile, "w"))

        self.wrapperCmd = [self.taskWrapper, runid, taskStr, argFile]



    def _run(self) :
        """
        Outer loop of _run() handles task retry behavior:
        """
        startTime = time.time()
        retries = 0
        retInfo = Bunch(retval=1, taskExitMsg="", isAllowRetry=False)

        while not self.stopped() :
            if retries :
                self.infoLog("Retrying task: '%s'. Total prior task failures: %i" % (self.taskStr, retries))

            if self.isDryRun :
                self.infoLog("Dryrunning task: '%s' task arg list: [%s]" % (self.taskStr, ",".join(['"%s"' % (s) for s in self.getFullCmd()])))
                retInfo.retval = 0
            else :
                self.runOnce(retInfo)

            if retInfo.retval == 0 : break
            if retries >= self.retry.max : break
            elapsed = (time.time() - startTime)
            if (self.retry.window > 0) and \
               (elapsed >= self.retry.window) : break
            if self.stopped() : break
            if not retInfo.isAllowRetry : break
            retries += 1
            self.warningLog("Task: '%s' failed but qualifies for retry. Total task failures (including this one): %i. Task command: '%s'" % (self.taskStr, retries, str(self.cmd)))
            retInfo = Bunch(retval=1, taskExitMsg="", isAllowRetry=False)
            time.sleep(self.retry.wait)

        return (retInfo.retval, retInfo.taskExitMsg)


    def getExitMsg(self) :
        """
        Attempt to extract exit message from a failed command task, do not complain in
        case of any errors in task signal file for this case.
        """
        msgSize = None
        wrapFp = open(self.wrapFile)
        for line in wrapFp:
            w = line.strip().split()
            if (len(w) < 6) or (w[4] != "[wrapperSignal]") :
                break
            if w[5] == "taskStderrTail" :
                if (len(w) == 7) : msgSize = int(w[6])
                break

        taskExitMsg = ""
        if msgSize is not None :
            i = 0
            for line in wrapFp:
                if i >= msgSize : break
                taskExitMsg += line
                i += 1
        wrapFp.close()
        return taskExitMsg


    def getWrapFileResult(self) :
        """
        When the task is theoretically done, go and read the task wrapper to
        see the actual task exit code. This is required because:

        1) On SGE or similar: We have no other way to get the exit code

        2) On all systems, we can distinguish between a conventional task error
        and other problems, such as (a) linux OOM killer (b) exception in the
        task wrapper itself (c) filesystem failures.
        """

        def checkWrapFileExit(result) :
            """
            return isError=True on error in file format only, missing or incomplete file
            is not considered an error and the function should not return an error for this
            case.
            """

            if not os.path.isfile(self.wrapFile) : return

            for line in open(self.wrapFile) :
                # an incomplete line indicates that the file is still being written:
                if len(line) == 0 or line[-1] != '\n' : return

                w = line.strip().split()

                if len(w) < 6 :
                    result.isError = True
                    return
                if (w[4] != "[wrapperSignal]") :
                    result.isError = True
                    return
                if w[5] == "taskExitCode" :
                    if (len(w) == 7) :
                        result.taskExitCode = int(w[6])
                    return

        retryCount = 8
        retryDelaySec = 30

        wrapResult = Bunch(taskExitCode=None, isError=False)

        totalDelaySec = 0
        for trialIndex in range(retryCount) :
            # if the problem occurs at 0 seconds don't bother with a warning, but
            # if we've gone through a full retry cycle, then the filesystem delay is
            # getting unusual and should be a warning:
            if trialIndex > 1 :
                msg = "No complete signal file found after %i seconds, retrying after delay. Signal file path: '%s'" % (totalDelaySec,self.wrapFile)
                self.flowLog(msg, logState=LogState.WARNING)

            if trialIndex != 0 :
                time.sleep(retryDelaySec)
                totalDelaySec += retryDelaySec

            checkWrapFileExit(wrapResult)
            if wrapResult.isError : break
            if wrapResult.taskExitCode is not None : break

        return wrapResult


    def getWrapperErrorMsg(self) :
        if os.path.isfile(self.wrapFile) :
            stderrList = open(self.wrapFile).readlines()
            taskExitMsg = ["Anomalous task wrapper stderr output. Wrapper signal file: '%s'" % (self.wrapFile),
                                  "Logging %i line(s) of task wrapper log output below:" % (len(stderrList))]
            linePrefix = "[taskWrapper-stderr]"
            taskExitMsg.extend([linePrefix + " " + line for line in stderrList])
        else :
            taskExitMsg = ["Anomalous task wrapper condition: Wrapper signal file is missing: '%s'" % (self.wrapFile)]

        return taskExitMsg



class LocalTaskRunner(CommandTaskRunner) :

    def getFullCmd(self) :
        return [sys.executable] + self.wrapperCmd

    def runOnce(self, retInfo) :
        #        sys.stderr.write("starting subprocess call. task '%s' cmd '%s'" % (self.taskStr,self.cmd))
        #        sys.stderr.write("full cmd: "+" ".join(self.getFullCmd()) + "\n")
        wrapFp = open(self.wrapFile, "w")
        proc = subprocess.Popen(self.getFullCmd(), stdout=wrapFp, stderr=subprocess.STDOUT, shell=False, bufsize=1)
        self.infoLog("Task initiated on local node")
        retInfo.retval = proc.wait()
        wrapFp.close()

        wrapResult = self.getWrapFileResult()

        if (wrapResult.taskExitCode is None) or (wrapResult.taskExitCode != retInfo.retval):
            retInfo.taskExitMsg = self.getWrapperErrorMsg()
            retInfo.retval = 1
            return retInfo
        elif retInfo.retval != 0 :
            retInfo.taskExitMsg = self.getExitMsg()

        retInfo.isAllowRetry = True

        # success! (taskWrapper, but maybe not for the task...)
        return retInfo



class QCaller(threading.Thread) :
    """
    Calls to both qsub and qstat go through this run() method so that we
    can time them out:
    """

    def __init__(self, cmd, infoLog) :
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.setName("QCaller-Timeout-Thread")
        self.lock = threading.RLock()
        self.cmd = cmd
        self.infoLog = infoLog
        self.results = Bunch(isComplete=False, retval=1, outList=[])
        self.proc = None
        self.is_kill_attempt = False

    def run(self) :
        # Note: Moved Popen() call outside of the mutex and
        # stopped using proc.communicate() here after
        # observing python interpreter bug:
        # http://bugs.python.org/issue13817
        #
        # The interpreter deadlock for this issue has been
        # observed to block the Popen() call below when using
        # python 2.7.2:
        #
        # Oct 2014 - also wrapped this call with a semaphore because
        # of the high memory usage associated with each qsub/qstat
        # subprocess. This was causing pyflow jobs to become unstable
        # as they would spontaneously exceed the maximum allowed master
        # process memory.
        #
        GlobalSync.subprocessControl.acquire()
        try :
            tmp_proc = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
            self.lock.acquire()
            try:
                self.proc = tmp_proc
                # handle the case where Popen was taking its good sweet time and a killProc() was sent in the meantime:
                if self.is_kill_attempt: self.killProc()
            finally:
                self.lock.release()

            if self.is_kill_attempt: return

            for line in self.proc.stdout :
                self.results.outList.append(line)
            self.results.retval = self.proc.wait()
        finally:
            GlobalSync.subprocessControl.release()
        self.results.isComplete = True

    @lockMethod
    def killProc(self) :
        import signal

        self.is_kill_attempt = True

        if self.proc is None : return

        try:
            os.kill(self.proc.pid , signal.SIGTERM)
            self.infoLog("Sent SIGTERM to sge command process id: %i" % (self.proc.pid))
        except OSError :
            # process ended before we could kill it (hopefully rare, but possible race condition artifact)
            pass



class SGETaskRunner(CommandTaskRunner) :

    def getFullCmd(self):
        # qsub options:
        #
        qsubCmd = ["qsub",
                 "-V",  # import environment variables from shell
                 "-cwd",  # use current working directory
                 "-S", sys.executable,  # The taskwrapper script is python
                 "-o", self.wrapFile,
                 "-e", self.wrapFile]

        qsubCmd.extend(self.schedulerArgList)
        qsubCmd.extend(siteConfig.qsubResourceArg(self.nCores, self.memMb))
        qsubCmd.extend(self.wrapperCmd)

        return tuple(qsubCmd)


    def setInitialRunstate(self) :
        self.setRunstate("queued")


    @lockMethod
    def setNewJobId(self, jobId) :
        """
        if stopped here, this is the case where a ctrl-c was entered while the qsub
        command was being submitted, so we must kill the job here:
        """
        self.jobId = jobId
        if self.stopped(): self._killJob()


    def runOnce(self, retInfo) :

        def qcallWithTimeouts(cmd, maxQcallAttempt=1) :
            maxQcallWait = 180
            qcall = None
            for i in range(maxQcallAttempt) :
                qcall = QCaller(cmd,self.infoLog)
                qcall.start()
                qcall.join(maxQcallWait)
                if not qcall.isAlive() : break
                self.infoLog("Trial %i of sge command has timed out. Killing process for cmd '%s'" % ((i + 1), cmd))
                qcall.killProc()
                self.infoLog("Finished attempting to kill sge command")

            return qcall.results

        # 1) call qsub, check for errors and retrieve taskId:
        #
        if os.path.isfile(self.wrapFile): os.remove(self.wrapFile)

        # write extra info, just in case we need it for post-mortem debug:
        qsubFile = os.path.join(os.path.dirname(self.wrapFile), "qsub.args.txt")
        if os.path.isfile(qsubFile): os.remove(qsubFile)
        qsubfp = open(qsubFile, "w")
        for arg in self.getFullCmd() :
            qsubfp.write(arg + "\n")
        qsubfp.close()

        results = qcallWithTimeouts(self.getFullCmd())

        isQsubError = False
        self.jobId = None
        if len(results.outList) != 1 :
            isQsubError = True
        else :
            w = results.outList[0].split()
            if (len(w) > 3) and (w[0] == "Your") and (w[1] == "job") :
                self.setNewJobId(int(w[2]))
            else :
                isQsubError = True

        if not results.isComplete :
            self._killJob()  # just in case...
            retInfo.taskExitMsg = ["Job submission failure -- qsub command timed-out"]
            return retInfo

        if isQsubError or (self.jobId is None):
            retInfo.taskExitMsg = ["Unexpected qsub output. Logging %i line(s) of qsub output below:" % (len(results.outList)) ]
            retInfo.taskExitMsg.extend([ "[qsub-out] " + line for line in results.outList ])
            return retInfo

        if results.retval != 0 :
            retInfo.retval = results.retval
            retInfo.taskExitMsg = ["Job submission failure -- qsub returned exit code: %i" % (retInfo.retval)]
            return retInfo

        # No qsub errors detected and an sge job_number is acquired -- success!
        self.infoLog("Task submitted to sge queue with job_number: %i" % (self.jobId))


        # 2) poll jobId until sge indicates it's not running or queued:
        #
        queueStatus = Bunch(isQueued=True, runStartTimeStamp=None)

        def checkWrapFileRunStart(result) :
            """
            check wrapper file for a line indicating that it has transitioned from queued to
            running state. Allow for NFS delay or incomplete file
            """
            if not os.path.isfile(self.wrapFile) : return
            for line in open(self.wrapFile) :
                w = line.strip().split()
                if (len(w) < 6) or (w[4] != "[wrapperSignal]") :
                    # this could be incomplete flush to the signal file, so
                    # don't treat it as error:
                    return
                if w[5] == "taskStart" :
                    result.runStartTimeStamp = timeStrToTimeStamp(w[0].strip('[]'))
                    result.isQueued = False
                    return


        # exponential polling times -- make small jobs responsive but give sge a break on long runs...
        ewaiter = ExpWaiter(5, 1.7, 60)

        pollCmd = ("/bin/bash", "--noprofile", "-o", "pipefail", "-c", "qstat -j %i | awk '/^error reason/'" % (self.jobId))
        while not self.stopped():
            results = qcallWithTimeouts(pollCmd, 6)
            isQstatError = False
            if results.retval != 0:
                if ((len(results.outList) == 2) and
                     (results.outList[0].strip() == "Following jobs do not exist:") and
                     (int(results.outList[1]) == self.jobId)) :
                    break
                else :
                    isQstatError = True
            else :
                if (len(results.outList) != 0) :
                    isQstatError = True

            if isQstatError :
                if not results.isComplete :
                    retInfo.taskExitMsg = ["The qstat command for sge job_number %i has timed out for all attempted retries" % (self.jobId)]
                    self._killJob()
                else :
                    retInfo.taskExitMsg = ["Unexpected qstat output or task has entered sge error state. Sge job_number: %i" % (self.jobId)]
                    retInfo.taskExitMsg.extend(["Logging %i line(s) of qstat output below:" % (len(results.outList)) ])
                    retInfo.taskExitMsg.extend([ "[qstat-out] " + line for line in results.outList ])
                    # self._killJob() # leave the job there so the user can better diagnose whetever unexpected pattern has occurred
                return retInfo

            # also check to see if job has transitioned from queued to running state:
            if queueStatus.isQueued :
                checkWrapFileRunStart(queueStatus)
                if not queueStatus.isQueued :
                    self.setRunstate("running", queueStatus.runStartTimeStamp)

            ewaiter.wait()

        if self.stopped() :
            # self._killJob() # no need, job should already have been killed at the stop() call...
            return retInfo

        lastJobId = self.jobId

        # if we've correctly communicated with SGE, then its roll is done here
        # if a job kill is required for any of the error states above, it needs to be
        # added before this point:
        self.jobId = None

        wrapResult = self.getWrapFileResult()

        if wrapResult.taskExitCode is None :
            retInfo.taskExitMsg = ["Sge job_number: '%s'" % (lastJobId)]
            retInfo.taskExitMsg.extend(self.getWrapperErrorMsg())
            retInfo.retval = 1
            return retInfo
        elif wrapResult.taskExitCode != 0 :
            retInfo.taskExitMsg = self.getExitMsg()

        retInfo.retval = wrapResult.taskExitCode
        retInfo.isAllowRetry = True

        # success! (for sge & taskWrapper, but maybe not for the task...)
        return retInfo


    @lockMethod
    def _killJob(self) :
        """
        (possibly) asynchronous job kill
        """
        try                  : isKilled = self.isKilled
        except AttributeError: isKilled = False
        if isKilled: return

        try                  : jobId = self.jobId
        except AttributeError: jobId = None
        if jobId is None: return
        killCmd = ["qdel", "%i" % (int(jobId))]
        # don't wait for or check exit code of kill cmd... just give it one try
        # because we want cleanup to go as quickly as possible
        subprocess.Popen(killCmd, shell=False)
        self.isKilled = True


    @lockMethod
    def stop(self) :
        """
        overload thead stop function to provide a
        qdel any running tasks.
        """
        CommandTaskRunner.stop(self)
        self._killJob()



class TaskManager(StoppableThread) :
    """
    This class runs on a separate thread from workflowRunner,
    launching jobs based on the current state of the TaskDAG
    """

    def __init__(self, cdata, tdag) :
        """
        @param cdata: data from WorkflowRunner instance which will be
                 constant during the lifetime of the TaskManager,
                 should be safe to lookup w/o locking
        @param tdag: task graph
        """
        StoppableThread.__init__(self)
        # parameter copy:
        self._cdata = cdata
        self.tdag = tdag
        # thread settings:
        self.setDaemon(True)
        self.setName("TaskManager-Thread")
        # lock is used for function (harvest), which is checked by
        # the WorkflowRunner under (literally) exceptional circumstances only
        self.lock = threading.RLock()
        # rm configuration:
        self.freeCores = self._cdata.param.nCores
        self.freeMemMb = self._cdata.param.memMb
        self.runningTasks = {}

        # This is used to track 'pyflow mutexes' -- for each key only a single
        # task can run at once. Key is set to True if mutex is occupied.
        self.taskMutexState = {}



    def run(self) :
        """
        TaskManager runs so long as there are outstanding jobs
        """

        try:
            cleanEnv()
            while not self._isTerm() :
                # update status of running jobs
                self.tdag.isFinishedEvent.clear()
                self.harvestTasks()
                # try to launch jobs:
                if self.stopped() : continue
                self._startTasks()
                self.tdag.isFinishedEvent.wait(5)
        except:
            msg = getExceptionMsg()
            self._flowLog(msg,logState=LogState.ERROR)
            self._cdata.emailNotification(msg, self._flowLog)
            self._cdata.setTaskManagerException()


    def _getCommandTaskRunner(self, task) :
        """
        assist launch of a command-task
        """

        # shortcuts:
        payload = task.payload
        param = self._cdata.param

        if payload.cmd is None :
            # Note these should have been marked off by the TaskManager already:
            raise Exception("Attempting to launch checkpoint task: %s" % (task.fullLabel()))

        isForcedLocal = ((param.mode != "local") and (payload.isForceLocal))

        # mark task resources as occupied:
        if not isForcedLocal :
            if self.freeCores != "unlimited" :
                if (self.freeCores < payload.nCores) :
                    raise Exception("Not enough free cores to launch task")
                self.freeCores -= payload.nCores

            if self.freeMemMb != "unlimited" :
                if (self.freeMemMb < payload.memMb) :
                    raise Exception("Not enough free memory to launch task")
                self.freeMemMb -= payload.memMb

        if payload.mutex is not None :
            self.taskMutexState[payload.mutex] = True

        TaskRunner = None
        if param.mode == "local" or payload.isForceLocal or payload.isCmdMakePath :
            TaskRunner = LocalTaskRunner
        elif param.mode == "sge" :
            TaskRunner = SGETaskRunner
        else :
            raise Exception("Can't support mode: '%s'" % (param.mode))

        #
        # TODO: find less hacky way to handle make tasks:
        #
        taskRetry = payload.retry

        if payload.isCmdMakePath :
            taskRetry = copy.deepcopy(payload.retry)
            taskRetry.window = 0

            if param.mode == "local" or payload.isForceLocal :
                launchCmdList = ["make", "-j", str(payload.nCores)]
            elif param.mode == "sge" :
                launchCmdList = siteConfig.getSgeMakePrefix(payload.nCores, payload.memMb, param.schedulerArgList)
            else :
                raise Exception("Can't support mode: '%s'" % (param.mode))

            launchCmdList.extend(["-C", payload.cmd.cmd])
            payload.launchCmd = Command(launchCmdList, payload.cmd.cwd, payload.cmd.env)

        #
        # each commandTaskRunner requires a unique tmp dir to write
        # wrapper signals to. TaskRunner will create this directory -- it does not bother to destroy it right now:
        #

        # split the task id into two parts to keep from adding too many files to one directory:
        tmpDirId1 = "%03i" % ((int(task.id) / 1000))
        tmpDirId2 = "%03i" % ((int(task.id) % 1000))
        taskRunnerTmpDir = os.path.join(self._cdata.wrapperLogDir, tmpDirId1, tmpDirId2)

        return TaskRunner(task.runStatus, self._cdata.getRunid(),
                          task.fullLabel(), payload.launchCmd,
                          payload.nCores, payload.memMb,
                          taskRetry, param.isDryRun,
                          self._cdata.taskStdoutFile,
                          self._cdata.taskStderrFile,
                          taskRunnerTmpDir,
                          param.schedulerArgList,
                          self._cdata.flowLog,
                          task.setRunstate)


    def _getWorkflowTaskRunner(self, task) :
        """
        assist launch of a workflow-task
        """
        return WorkflowTaskRunner(task.runStatus, task.fullLabel(), task.payload.workflow,
                                  self._cdata.flowLog, task.setRunstate)


    def _launchTask(self, task) :
        """
        launch a specific task
        """

        if   task.payload.type() == "command" :
            trun = self._getCommandTaskRunner(task)
        elif task.payload.type() == "workflow" :
            trun = self._getWorkflowTaskRunner(task)
        else :
            assert 0

        self._infoLog("Launching %s: '%s' from %s" % (task.payload.desc(), task.fullLabel(), namespaceLabel(task.namespace)))
        trun.start()
        self.runningTasks[task] = trun


    @lockMethod
    def _startTasks(self) :
        """
        determine what tasks, if any, can be started

        Note that the lock is here to protect self.runningTasks
        """
        # trace through DAG, completing any empty-command checkpoints
        # found with all dependencies completed:
        (ready, completed) = self.tdag.getReadyTasks()
        for node in completed:
            if self.stopped() : return
            self._infoLog("Completed %s: '%s' launched from %s" % (node.payload.desc(), node.fullLabel(), namespaceLabel(node.namespace)))

        # launch all workflows first, then command tasks as resources
        # allow:
        ready_workflows = [r for r in ready if r.payload.type() == "workflow"]
        for task in ready_workflows :
            if self.stopped() : return
            self._launchTask(task)

        # task submission could be shutdown, eg. in response to a task
        # error:
        if (not self._cdata.isTaskSubmissionActive()) : return

        isNonLocal = (self._cdata.param.mode != "local")

        # start command task launch:
        ready_commands = [r for r in ready if r.payload.type() == "command"]
        ready_commands.sort(key=lambda t: (t.payload.priority, t.payload.nCores), reverse=True)
        for task in ready_commands :
            if self.stopped() : return

            # In a non-local run mode, "isForceLocal" tasks are not subject to
            # global core and memory restrictions:
            isForcedLocal = (isNonLocal and task.payload.isForceLocal)
            if not isForcedLocal :
                if ((self.freeCores != "unlimited") and (task.payload.nCores > self.freeCores)) : continue
                if ((self.freeMemMb != "unlimited") and (task.payload.memMb > self.freeMemMb)) : continue

            # all command tasks must obey separate mutex restrictions:
            if ((task.payload.mutex is not None) and
                (task.payload.mutex in self.taskMutexState) and
                (self.taskMutexState[task.payload.mutex])) : continue

            self._launchTask(task)



    @lockMethod
    def harvestTasks(self) :
        """
        Check the set of running tasks to see if they've completed and update
        Node status accordingly:
        """
        notrunning = set()
        for task in self.runningTasks.keys() :
            if self.stopped() : break
            trun = self.runningTasks[task]
            if not task.runStatus.isComplete.isSet() :
                if trun.isAlive() : continue
                # if not complete and thread is dead then we don't know what happened, very bad!:
                task.errorstate = 1
                task.errorMessage = "Thread: '%s', has stopped without a traceable cause" % (trun.getName())
            else :
                task.errorstate = task.runStatus.errorCode
                task.errorMessage = task.runStatus.errorMessage

            if task.errorstate == 0 :
                task.setRunstate("complete")
            else:
                task.setRunstate("error")

            notrunning.add(task)

            if not task.isError() :
                self._infoLog("Completed %s: '%s' launched from %s" % (task.payload.desc(), task.fullLabel(), namespaceLabel(task.namespace)))
            else:
                msg = task.getTaskErrorMsg()

                if self._cdata.isTaskSubmissionActive() :
                    # if this is the first error in the workflow, then
                    # we elaborate a bit on the workflow's response to
                    # the error. We also send any email-notifications
                    # for the first error only:
                    msg.extend(["Shutting down task submission. Waiting for remaining tasks to complete."])

                self._errorLog(msg)
                if self._cdata.isTaskSubmissionActive() :
                    self._cdata.emailNotification(msg, self._flowLog)

                # Be sure to send notifications *before* setting error
                # bits, because the WorkflowRunner may decide to
                # immediately shutdown all tasks and pyflow threads on
                # the first error:
                self._cdata.setTaskError(task)

        # shortcut:
        param = self._cdata.param

        # recover task resources:
        for task in notrunning :
            if task.payload.type() == "command" :
                isForcedLocal = ((param.mode != "local") and (task.payload.isForceLocal))
                if not isForcedLocal :
                    if self.freeCores != "unlimited" :
                        self.freeCores += task.payload.nCores
                    if self.freeMemMb != "unlimited" :
                        self.freeMemMb += task.payload.memMb

                if task.payload.mutex is not None :
                    self.taskMutexState[task.payload.mutex] = False

        for task in notrunning:
            del self.runningTasks[task]


    @lockMethod
    def stop(self) :
        StoppableThread.stop(self)
        for trun in self.runningTasks.values() :
            trun.stop()


    @lockMethod
    def _areTasksDead(self) :
        for trun in self.runningTasks.values() :
            if trun.isAlive(): return False
        return True


    def _isTerm(self) :
        # check for explicit thread stop request (presumably from the workflowManager):
        # if this happens we exit the polling loop
        #
        if self.stopped() :
            while True :
                if self._areTasksDead() : return True
                time.sleep(1)

        # check for "regular" termination conditions:
        if (not self._cdata.isTaskSubmissionActive()) :
            return (len(self.runningTasks) == 0)
        else :
            if self.tdag.isRunComplete() :
                if (len(self.runningTasks) != 0) :
                    raise Exception("Inconsistent TaskManager state: workflow appears complete but there are still running tasks")
                return True
            elif self.tdag.isRunExhausted() :
                return True
            else :
                return False


    def _flowLog(self, msg, logState) :
        linePrefixOut = "[TaskManager]"
        # if linePrefix is not None : linePrefixOut+=" "+linePrefix
        self._cdata.flowLog(msg, linePrefix=linePrefixOut, logState=logState)


    def _infoLog(self, msg) :
        self._flowLog(msg, logState=LogState.INFO)

    def _errorLog(self, msg) :
        self._flowLog(msg, logState=LogState.ERROR)




# payloads are used to manage the different
# possible actions attributed to task nodes:
#
class CmdPayload(object) :
    def __init__(self, fullLabel, cmd, nCores, memMb, priority,
                 isForceLocal, isCmdMakePath=False, isTaskStable=True,
                 mutex=None, retry=None) :
        self.cmd = cmd
        self.nCores = nCores
        self.memMb = memMb
        self.priority = priority
        self.isForceLocal = isForceLocal
        self.isCmdMakePath = isCmdMakePath
        self.isTaskStable = isTaskStable
        self.mutex = mutex
        self.retry = retry

        # launch command includes make/qmake wrapper for Make path commands:
        self.launchCmd = cmd

        if (cmd.cmd is None) and ((nCores != 0) or (memMb != 0)) :
            raise Exception("Null tasks should not have resource requirements. task: '%s'" % (fullLabel))

    def type(self) :
        return "command"

    def desc(self) :
        return "command task"


class WorkflowPayload(object) :
    def __init__(self, workflow) :
        self.workflow = workflow
        self.isTaskStable = True

    def type(self) :
        return "workflow"

    def name(self) :
        if self.workflow is None :
            return "None"
        else :
            return self.workflow._whoami()

    def desc(self) :
        return "sub-workflow task"



class TaskNode(object) :
    """
    Represents an individual task in the task graph
    """

    def __init__(self, tdag, lock, init_id, namespace, label, payload, isContinued, isFinishedEvent) :
        self.tdag = tdag
        self.lock = lock
        self.id = init_id
        self.namespace = namespace
        self.label = label
        self.payload = payload
        self.isContinued = isContinued

        # if true, do not execute this task or honor it as a dependency for child tasks
        self.isIgnoreThis = False

        # if true, set the ignore state for all children of this task to true
        self.isIgnoreChildren = False

        # if true, this task and its dependents will be automatically marked as completed (until
        # a startFromTasks node is found)
        self.isAutoCompleted = False

        # task is reset to waiting runstate in a continued run
        self.isReset = False

        self.parents = set()
        self.children = set()
        self.runstateUpdateTimeStamp = time.time()
        if self.isContinued:
            self.runstate = "complete"
        else:
            self.runstate = "waiting"
        self.errorstate = 0

        # errorMessage is used by sub-workflow tasks, but not by command taks:
        self.errorMessage = ""

        # This is a link to the live status object updated by TaskRunner:
        self.runStatus = RunningTaskStatus(isFinishedEvent)

    def __str__(self) :
        msg = "TASK id: %s state: %s error: %i" % (self.fullLabel(), self.runstate, self.errorstate)
        return msg

    def fullLabel(self) :
        return namespaceJoin(self.namespace, self.label)

    @lockMethod
    def isDone(self) :
        "task has gone as far as it can"
        return ((self.runstate == "error") or (self.runstate == "complete"))

    @lockMethod
    def isError(self) :
        "true if an error occurred in this node"
        return ((self.errorstate != 0) or (self.runstate == "error"))

    @lockMethod
    def isComplete(self) :
        "task completed without error"
        return ((self.errorstate == 0) and (self.runstate == "complete"))

    @lockMethod
    def isReady(self) :
        "task is ready to be run"
        retval = ((self.runstate == "waiting") and (self.errorstate == 0) and (not self.isIgnoreThis))
        if retval :
            for p in self.parents :
                if p.isIgnoreThis : continue
                if not p.isComplete() :
                    retval = False
                    break
        return retval


    def _isDeadWalker(self, searched) :
        "recursive helper function for isDead()"

        # the fact that you're still searching means that it must have returned False last time:
        if self in searched : return False
        searched.add(self)

        if self.isError() : return True
        if self.isComplete() : return False
        for p in self.parents :
            if p._isDeadWalker(searched) : return True
        return False

    @lockMethod
    def isDead(self) :
        """
        If true, there's no longer a point to waiting for this task,
        because it either has an error or there is an error in an
        upstream dependency
        """

        # searched is used to restrict the complexity of this
        # operation on large graphs:
        searched = set()
        return self._isDeadWalker(searched)

    @lockMethod
    def setRunstate(self, runstate, updateTimeStamp=None) :
        """
        updateTimeStamp is only supplied in the case where the state
        transition time is interestingly different than the function
        call time. This can happen with the state update comes from
        a polling function with a long poll interval.
        """
        if runstate not in TaskNodeConstants.validRunstates :
            raise Exception("Can't set TaskNode runstate to %s" % (runstate))

        if updateTimeStamp is None :
            self.runstateUpdateTimeStamp = time.time()
        else :
            self.runstateUpdateTimeStamp = updateTimeStamp
        self.runstate = runstate
        self.tdag.writeTaskStatus()

    #def getParents(self) :
    #    return self.parents

    #def getChildren(self) :
    #    return self.children

    @lockMethod
    def getTaskErrorMsg(self) :
        """
        generate consistent task error message from task state
        """

        if not self.isError() : return []

        msg = "Failed to complete %s: '%s' launched from %s" % (self.payload.desc(), self.fullLabel(), namespaceLabel(self.namespace))
        if self.payload.type() == "command" :
            msg += ", error code: %s, command: '%s'" % (str(self.errorstate), str(self.payload.launchCmd))
        elif self.payload.type() == "workflow" :
            msg += ", failed sub-workflow classname: '%s'" % (self.payload.name())
        else :
            assert 0

        msg = lister(msg)

        if self.errorMessage != "" :
            msg2 = ["Error Message:"]
            msg2.extend(lister(self.errorMessage))
            linePrefix = "[%s] " % (self.fullLabel())
            for i in range(len(msg2)) :
                msg2[i] = linePrefix + msg2[i]
            msg.extend(msg2)

        return msg



class TaskDAG(object) :
    """
    Holds all tasks and their dependencies.

    Also responsible for task state persistence/continue across
    interrupted runs. Object is accessed by both the workflow and
    taskrunner threads, so it needs to be thread-safe.
    """

    def __init__(self, isContinue, isForceContinue, isDryRun,
                 taskInfoFile, taskStateFile, workflowClassName,
                 startFromTasks, ignoreTasksAfter, resetTasks,
                 flowLog) :
        """
        No other object gets to access the taskStateFile, file locks
        are not required (but thread locks are)
        """
        self.isContinue = isContinue
        self.isForceContinue = isForceContinue
        self.isDryRun = isDryRun
        self.taskInfoFile = taskInfoFile
        self.taskStateFile = taskStateFile
        self.workflowClassName = workflowClassName
        self.startFromTasks = startFromTasks
        self.ignoreTasksAfter = ignoreTasksAfter
        self.resetTasks = resetTasks
        self.flowLog = flowLog

        # unique id for each task in each run -- not persistent across continued runs:
        self.taskId = 0

        # it will be easier for people to read the task status file if
        # the tasks are in approximately the same order as they were
        # added by the workflow:
        self.addOrder = []
        self.labelMap = {}
        self.headNodes = set()
        self.tailNodes = set()
        self.lock = threading.RLock()

        # this event can be used to optionally accelerate the task cycle
        # when running in modes where task can set this event on completion
        # (ie. local mode but not sge), if this isn't set the normal polling
        # cycle applies
        self.isFinishedEvent = threading.Event()

    @lockMethod
    def isTaskPresent(self, namespace, label) :
        return ((namespace, label) in self.labelMap)

    @lockMethod
    def getTask(self, namespace, label) :
        if (namespace, label) in self.labelMap :
            return self.labelMap[(namespace, label)]
        return None

    @lockMethod
    def getHeadNodes(self) :
        "all tasks with no parents"
        return list(self.headNodes)

    @lockMethod
    def getTailNodes(self) :
        "all tasks with no (runnable) children"
        return list(self.tailNodes)

    @lockMethod
    def getAllNodes(self, namespace="") :
        "get all nodes in this namespace"
        retval = []
        for (taskNamespace, taskLabel) in self.addOrder :
            if namespace != taskNamespace : continue
            node=self.labelMap[(taskNamespace, taskLabel)]
            if node.isIgnoreThis : continue
            retval.append(node)
        return retval

    def _isRunExhaustedNode(self, node, searched) :

        # the fact that you're still searching means that it must have returned true last time:
        if node in searched : return True
        searched.add(node)

        if not node.isIgnoreThis :
            if not node.isDone() :
                return False
            if node.isComplete() :
                for c in node.children :
                    if not self._isRunExhaustedNode(c, searched) :
                        return False
        return True

    @lockMethod
    def isRunExhausted(self) :
        """
        Returns true if the run is as complete as possible due to errors
        """

        # searched is used to restrict the complexity of this
        # operation on large graphs:
        searched = set()
        for node in self.getHeadNodes() :
            if not self._isRunExhaustedNode(node,searched) :
                return False
        return True


    @lockMethod
    def isRunComplete(self) :
        "returns true if run is complete and error free"
        for node in self.labelMap.values():
            if node.isIgnoreThis : continue
            if not node.isComplete() :
                return False
        return True


    def _getReadyTasksFromNode(self, node, ready, searched) :
        "helper function for getReadyTasks"

        if node.isIgnoreThis : return

        if node in searched : return
        searched.add(node)

        if node.isReady() :
            ready.add(node)
        else:
            if not node.isComplete() :
                for c in node.parents :
                    self._getReadyTasksFromNode(c, ready, searched)


    @lockMethod
    def getReadyTasks(self) :
        """
        Go through DAG from the tail nodes and find all tasks which
        have all prerequisites completed:
        """

        completed = self.markCheckPointsComplete()
        ready = set()
        # searched is used to restrict the complexity of this
        # operation on large graphs:
        searched = set()
        for node in self.getTailNodes() :
            self._getReadyTasksFromNode(node, ready, searched)
        return (list(ready), list(completed))


    def _markCheckPointsCompleteFromNode(self, node, completed, searched) :
        "helper function for markCheckPointsComplete"

        if node.isIgnoreThis : return

        if node in searched : return
        searched.add(node)

        if (node.payload.type() == "command") and (node.payload.cmd.cmd is None) and (node.isReady()) :
            node.setRunstate("complete")
            completed.add(node)

        if node.isComplete() :
            for c in node.children :
                self._markCheckPointsCompleteFromNode(c, completed, searched)


    @lockMethod
    def markCheckPointsComplete(self) :
        """
        traverse from head nodes down, marking any checkpoints
        (task.cmd=None) jobs that are ready as complete:
        """
        completed = set()
        # searched is used to restrict the complexity of this
        # operation on large graphs:
        searched = set()
        for node in self.getHeadNodes() :
            self._markCheckPointsCompleteFromNode(node, completed, searched)
        return completed


    @lockMethod
    def addTask(self, namespace, label, payload, dependencies, isContinued=False) :
        """
        add new task to the DAG

        isContinued indicates the task is being read from state history during a continuation run
        """
        # internal data structures use these separately, but for logging we
        # create one string:
        fullLabel = namespaceJoin(namespace, label)

        # first check to see if task exists in DAG already, this is not allowed unless
        # we are continuing a previous run, in which case it's allowed once:
        if not isContinued and self.isTaskPresent(namespace, label):
            if self.isContinue and self.labelMap[(namespace, label)].isContinued:
                # confirm that task is a match, flip off the isContinued flag and return:
                task = self.labelMap[(namespace, label)]
                parentLabels = set([p.label for p in task.parents])
                excPrefix = "Task: '%s' does not match previous definition defined in '%s'." % (fullLabel, self.taskInfoFile)
                if task.payload.type() != payload.type() :
                    msg = excPrefix + " New/old payload type: '%s'/'%s'" % (payload.type(), task.payload.type())
                    raise Exception(msg)
                if payload.isTaskStable :
                    if (payload.type() == "command") and (str(task.payload.cmd) != str(payload.cmd)) :
                        msg = excPrefix + " New/old command: '%s'/'%s'" % (str(payload.cmd), str(task.payload.cmd))
                        if self.isForceContinue : self.flowLog(msg,logState=LogState.WARNING)
                        else :                    raise Exception(msg)
                    if (parentLabels != set(dependencies)) :
                        msg = excPrefix + " New/old dependencies: '%s'/'%s'" % (",".join(dependencies), ",".join(parentLabels))
                        if self.isForceContinue : self.flowLog(msg,logState=LogState.WARNING)
                        else :                    raise Exception(msg)
                if payload.type() == "command" :
                    task.payload.cmd = payload.cmd
                    task.payload.isCmdMakePath = payload.isCmdMakePath
                task.isContinued = False
                return
            else:
                raise Exception("Task: '%s' is already in TaskDAG" % (fullLabel))

        task = TaskNode(self, self.lock, self.taskId, namespace, label, payload, isContinued, self.isFinishedEvent)

        self.taskId += 1

        self.addOrder.append((namespace, label))
        self.labelMap[(namespace, label)] = task

        for d in dependencies :
            parent = self.getTask(namespace, d)
            if parent is task :
                raise Exception("Task: '%s' cannot specify its own task label as a dependency" % (fullLabel))
            if parent is None :
                raise Exception("Dependency: '%s' for task: '%s' does not exist in TaskDAG" % (namespaceJoin(namespace, d), fullLabel))
            task.parents.add(parent)
            parent.children.add(task)


        if isContinued :
            isReset=False
            if label in self.resetTasks :
                isReset=True
            else :
                for p in task.parents :
                    if p.isReset :
                        isReset = True
                        break
            if isReset :
                task.setRunstate("waiting")
                task.isReset=True

        if not isContinued:
            self.writeTaskInfo(task)
            self.writeTaskStatus()

        # determine if this is an ignoreTasksAfter node
        if label in self.ignoreTasksAfter :
            task.isIgnoreChildren = True

        # determine if this is an ignoreTasksAfter descendent
        for p in task.parents :
            if p.isIgnoreChildren :
                task.isIgnoreThis = True
                task.isIgnoreChildren = True
                break

        # update headNodes
        if len(task.parents) == 0 :
            self.headNodes.add(task)

        # update isAutoCompleted:
        if (self.startFromTasks and
            (label not in self.startFromTasks)) :
            task.isAutoCompleted = True
            for p in task.parents :
                if not p.isAutoCompleted :
                    task.isAutoCompleted = False
                    break

            #  in case of no-parents, also check sub-workflow node
            if task.isAutoCompleted and (len(task.parents) == 0) and (namespace != ""):
                wval=namespace.rsplit(namespaceSep,1)
                if len(wval) == 2 :
                    (workflowNamespace,workflowLabel)=wval
                else :
                    workflowNamespace=""
                    workflowLabel=wval[0]
                workflowParent = self.labelMap[(workflowNamespace, workflowLabel)]
                if not workflowParent.isAutoCompleted :
                    task.isAutoCompleted = False

        if task.isAutoCompleted :
            task.setRunstate("complete")

        # update tailNodes:
        if not task.isIgnoreThis :
            self.tailNodes.add(task)
            for p in task.parents :
                if p in self.tailNodes :
                    self.tailNodes.remove(p)

        # check dependency runState consistency:
        if task.isDone() :
            for p in task.parents :
                if p.isIgnoreThis : continue
                if p.isComplete() : continue
                raise Exception("Task: '%s' has invalid continuation state. Task dependencies are incomplete")



    @lockMethod
    def writeTaskStatus(self) :
        """
        (atomic on *nix) update of the runstate and errorstate for all tasks
        """
        # don't write task status during dry runs:
        if self.isDryRun : return

        tmpFile = self.taskStateFile + ".update.incomplete"
        tmpFp = open(tmpFile, "w")
        tmpFp.write(taskStateHeader())
        for (namespace, label) in self.addOrder :
            node = self.labelMap[(namespace, label)]
            runstateUpdateTimeStr = timeStampToTimeStr(node.runstateUpdateTimeStamp)
            tmpFp.write("%s\t%s\t%s\t%i\t%s\n" % (label, namespace, node.runstate, node.errorstate, runstateUpdateTimeStr))
        tmpFp.close()

        forceRename(tmpFile, self.taskStateFile)


    @lockMethod
    def getTaskStatus(self) :
        """
        Enumerate status of command tasks (but look at sub-workflows to determine if specification is complete)
        """

        val = Bunch(waiting=0, queued=0, running=0, complete=0, error=0, isAllSpecComplete=True,
                    longestQueueSec=0, longestRunSec=0, longestQueueName="", longestRunName="")

        currentSec = time.time()
        for (namespace, label) in self.addOrder :
            node = self.labelMap[(namespace, label)]
            # special check just for workflow tasks:
            if node.payload.type() == "workflow" :
                if not node.runStatus.isSpecificationComplete.isSet() :
                    val.isAllSpecComplete = False

                # the rest of this enumeration is for command tasks only:
                continue

            taskTime = int(currentSec - node.runstateUpdateTimeStamp)

            if node.runstate == "waiting" :
                val.waiting += 1
            elif node.runstate == "queued" :
                val.queued += 1
                if val.longestQueueSec < taskTime :
                    val.longestQueueSec = taskTime
                    val.longestQueueName = node.fullLabel()
            elif node.runstate == "running" :
                val.running += 1
                if val.longestRunSec < taskTime :
                    val.longestRunSec = taskTime
                    val.longestRunName = node.fullLabel()
            elif node.runstate == "complete" :
                val.complete += 1
            elif node.runstate == "error" :
                val.error += 1

        return val


    @lockMethod
    def writeTaskInfo(self, task) :
        """
        appends a description of new tasks to the taskInfo file
        """
        depstring = ""
        if len(task.parents) :
            depstring = ",".join([p.label for p in task.parents])

        cmdstring = ""
        nCores = "0"
        memMb = "0"
        priority = "0"
        isForceLocal = "0"
        payload = task.payload
        cwdstring = ""
        if   payload.type() == "command" :
            cmdstring = str(payload.cmd)
            nCores = str(payload.nCores)
            memMb = str(payload.memMb)
            priority = str(payload.priority)
            isForceLocal = boolToStr(payload.isForceLocal)
            cwdstring = payload.cmd.cwd
        elif payload.type() == "workflow" :
            cmdstring = payload.name()
        else :
            assert 0
        taskline = "\t".join((task.label, task.namespace, payload.type(),
                            nCores, memMb, priority,
                            isForceLocal, depstring, cwdstring, cmdstring))
        fp = open(self.taskInfoFile, "a")
        fp.write(taskline + "\n")
        fp.close()



# workflowRunner:
#


# special exception used for the case where pyflow data dir is already in use:
#
class DataDirException(Exception) :
    def __init__(self, msg) :
        Exception.__init__(self)
        self.msg = msg



class WorkflowRunnerThreadSharedData(object) :
    """
    All data used by the WorkflowRunner which will be constant over
    the lifetime of a TaskManager instance. All of the information in
    this class will be accessed by both threads without locking.
    """

    def __init__(self) :
        self.lock = threading.RLock()
        self.pid = os.getpid()
        self.runcount = 0
        self.cwd = os.path.abspath(os.getcwd())

        self.markFile = None

        # we potentially have to log before the logfile is setup (eg
        # an exception is thrown reading run parameters), so provide
        # an explicit notification that there's no log file:
        self.flowLogFp = None

        self.warningLogFp = None
        self.errorLogFp = None

        self.resetRun()

        # two elements required to implement a nohup-like behavior:
        self.isHangUp = threading.Event()
        self._isStderrAlive = True


    @staticmethod
    def _validateFixParam(param):
        """
        validate and refine raw run() parameters for use by workflow
        """

        param.mailTo = setzer(param.mailTo)
        param.schedulerArgList = lister(param.schedulerArgList)
        if param.successMsg is not None :
            if not isString(param.successMsg) :
                raise Exception("successMsg argument to WorkflowRunner.run() is not a string")

        # create combined task retry settings manager:
        param.retry=RetryParam(param.mode,
                               param.retryMax,
                               param.retryWait,
                               param.retryWindow,
                               param.retryMode)

        # setup resource parameters
        if param.nCores is None :
            param.nCores = RunMode.data[param.mode].defaultCores

        # ignore total available memory settings in non-local modes:
        if param.mode != "local" :
            param.memMb = "unlimited"

        if param.mode == "sge" :
            if siteConfig.maxSGEJobs != "unlimited" :
                if ((param.nCores == "unlimited") or
                    (int(param.nCores) > int(siteConfig.maxSGEJobs))) :
                    param.nCores = int(siteConfig.maxSGEJobs)

        if param.nCores != "unlimited" :
            param.nCores = int(param.nCores)
            if param.nCores < 1 :
                raise Exception("Invalid run mode nCores argument: %s. Value must be 'unlimited' or an integer no less than 1" % (param.nCores))

        if param.memMb is None :
            if param.nCores == "unlimited" :
                param.memMb = "unlimited"
            mpc = RunMode.data[param.mode].defaultMemMbPerCore
            if mpc == "unlimited" :
                param.memMb = "unlimited"
            else :
                param.memMb = mpc * param.nCores
        elif param.memMb != "unlimited" :
            param.memMb = int(param.memMb)
            if param.memMb < 1 :
                raise Exception("Invalid run mode memMb argument: %s. Value must be 'unlimited' or an integer no less than 1" % (param.memMb))

        # verify/normalize input settings:
        if param.mode not in RunMode.data.keys() :
            raise Exception("Invalid mode argument '%s'. Accepted modes are {%s}." \
                            % (param.mode, ",".join(RunMode.data.keys())))

        if param.mode == "sge" :
            # TODO not-portable to windows (but is this a moot point -- all of sge mode is non-portable, no?):
            def checkSgeProg(prog) :
                proc = subprocess.Popen(("which", prog), stdout=open(os.devnull, "w"), shell=False)
                retval = proc.wait()
                if retval != 0 : raise Exception("Run mode is sge, but no %s in path" % (prog))
            checkSgeProg("qsub")
            checkSgeProg("qstat")


        stateDir = os.path.join(param.dataDir, "state")
        if param.isContinue == "Auto" :
            param.isContinue = os.path.exists(stateDir)

        if param.isContinue :
            if not os.path.exists(stateDir) :
                raise Exception("Cannot continue run without providing a pyflow dataDir containing previous state.: '%s'" % (stateDir))

        for email in param.mailTo :
            if not verifyEmailAddy(email):
                raise Exception("Invalid email address: '%s'" % (email))



    def _setCustomLogs(self) :
        if (self.warningLogFp is None) and (self.param.warningLogFile is not None) :
            self.warningLogFp = open(self.param.warningLogFile,"w")

        if (self.errorLogFp is None) and (self.param.errorLogFile is not None) :
            self.errorLogFp = open(self.param.errorLogFile,"w")



    def setupNewRun(self, param) :
        self.param = param

        # setup log file-handle first, then run the rest of parameter validation:
        # (hold this file open so that we can still log if pyflow runs out of filehandles)
        self.param.dataDir = os.path.abspath(self.param.dataDir)
        self.param.dataDir = os.path.join(self.param.dataDir, "pyflow.data")
        logDir = os.path.join(self.param.dataDir, "logs")
        ensureDir(logDir)
        self.flowLogFile = os.path.join(logDir, "pyflow_log.txt")
        self.flowLogFp = open(self.flowLogFile, "a")

        # run remaining validation
        self._validateFixParam(self.param)

        #  initial per-run data
        self.taskErrors = set()  # this set actually contains every task that failed -- tasks contain all of their own error info
        self.isTaskManagerException = False

        # create data directory if it does not exist
        ensureDir(self.param.dataDir)

        # check whether a process already exists:
        self.markFile = os.path.join(self.param.dataDir, "active_pyflow_process.txt")
        if os.path.exists(self.markFile) :
            # Non-conventional logging situation -- another pyflow process is possibly using this same data directory, so we want
            # to log to stderr (even if the user has set isQuiet) and not interfere with the other process's log
            self.flowLogFp = None
            self.param.isQuiet = False
            msg = [ "Can't initialize pyflow run because the data directory appears to be in use by another process.",
                    "\tData directory: '%s'" % (self.param.dataDir),
                    "\tIt is possible that a previous process was abruptly interrupted and did not clean up properly. To determine if this is",
                    "\tthe case, please refer to the file '%s'" % (self.markFile),
                    "\tIf this file refers to a non-running process, delete the file and relaunch pyflow,",
                    "\totherwise, specify a new data directory. At the API-level this can be done with the dataDirRoot option." ]
            self.markFile = None  # this keeps pyflow from deleting this file, as it normally would on exit
            raise DataDirException(msg)
        else :
            mfp = open(self.markFile, "w")
            msg = """
This file provides details of the pyflow instance currently using this data directory.
During normal pyflow run termination (due to job completion, error, SIGINT, etc...),
this file should be deleted. If this file is present it should mean either:
(1) the data directory is still in use by a running workflow
(2) a sudden job failure occurred that prevented normal run termination

The associated pyflow job details are as follows:
"""
            mfp.write(msg + "\n")
            for line in self.getInfoMsg() :
                mfp.write(line + "\n")
            mfp.write("\n")
            mfp.close()

        stateDir = os.path.join(self.param.dataDir, "state")
        ensureDir(stateDir)

        # setup other instance data:
        self.runcount += 1

        # initialize directories
        self.wrapperLogDir = os.path.join(logDir, "tmp", "taskWrapperLogs")
        ensureDir(self.wrapperLogDir)
        stackDumpLogDir = os.path.join(logDir, "tmp", "stackDumpLog")
        ensureDir(stackDumpLogDir)

        # initialize filenames:
        taskStateFileName = "pyflow_tasks_runstate.txt"
        taskInfoFileName = "pyflow_tasks_info.txt"

        self.taskStdoutFile = os.path.join(logDir, "pyflow_tasks_stdout_log.txt")
        self.taskStderrFile = os.path.join(logDir, "pyflow_tasks_stderr_log.txt")
        self.taskStateFile = os.path.join(stateDir, taskStateFileName)
        self.taskInfoFile = os.path.join(stateDir, taskInfoFileName)
        self.taskDotScriptFile = os.path.join(stateDir, "make_pyflow_task_graph.py")

        self.stackDumpLogFile = os.path.join(stackDumpLogDir, "pyflow_stack_dump.txt")

        # empty file:
        if not self.param.isContinue:
            fp = open(self.taskInfoFile, "w")
            fp.write(taskInfoHeader())
            fp.close()

        self._setCustomLogs()

        # finally write dot task graph creation script:
        #
        # this could fail because of script permission settings, buk it is not critical for
        # workflow completion so we get away with a warning
        try :
            writeDotScript(self.taskDotScriptFile, taskInfoFileName, taskStateFileName, self.param.workflowClassName)
        except OSError:
            msg = ["Failed to write task graph visualization script to %s" % (self.taskDotScriptFile)]
            self.flowLog(msg,logState=LogState.WARNING)


    def resetRun(self) :
        """
        Anything that needs to be cleaned up at the end of a run

        Right now this just make sure we don't log to the previous run's log file
        """
        self.flowLogFile = None
        self.param = None
        if self.flowLogFp is not None :
            self.flowLogFp.close()
            self.flowLogFp = None

        if self.warningLogFp is not None :
            self.warningLogFp.close()
            self.warningLogFp = None

        if self.errorLogFp is not None :
            self.errorLogFp.close()
            self.errorLogFp = None

        if self.markFile is not None :
            if os.path.exists(self.markFile) : os.unlink(self.markFile)

        self.markFile = None

    def getRunid(self) :
        return "%s_%s" % (self.pid, self.runcount)

    @lockMethod
    def setTaskError(self, task) :
        self.taskErrors.add(task)

    @lockMethod
    def isTaskError(self) :
        return (len(self.taskErrors) != 0)

    def isTaskSubmissionActive(self) :
        """
        wait() pollers need to know if task submission has been
        shutdown to implement sane behavior.
        """
        return (not self.isTaskError())

    @lockMethod
    def setTaskManagerException(self) :
        self.isTaskManagerException = True

    @lockMethod
    def flowLog(self, msg, linePrefix=None, logState = LogState.INFO) :
        linePrefixOut = "[%s]" % (self.getRunid())
        if linePrefix is not None :
            linePrefixOut += " " + linePrefix

        if (logState == LogState.ERROR) or (logState == LogState.WARNING) :
            linePrefixOut += " [" + LogState.toString(logState) + "]"

        ofpList = []
        isAddStderr = (self._isStderrAlive and ((self.flowLogFp is None) or (self.param is None) or (not self.param.isQuiet)))
        if isAddStderr:
            ofpList.append(sys.stderr)
        if self.flowLogFp is not None :
            ofpList.append(self.flowLogFp)

        # make a last ditch effort to open the special error logs if these are not available already:
        try :
            self._setCustomLogs()
        except :
            pass

        if (self.warningLogFp is not None) and (logState == LogState.WARNING) :
            ofpList.append(self.warningLogFp)
        if (self.errorLogFp is not None) and (logState == LogState.ERROR) :
            ofpList.append(self.errorLogFp)

        if len(ofpList) == 0 : return
        retval = log(ofpList, msg, linePrefixOut)

        # check if stderr stream failed. If so, turn it off for the remainder of run (assume terminal hup):
        if isAddStderr and (not retval[0]) :
            if self.isHangUp.isSet() :
                self._isStderrAlive = False


    def getInfoMsg(self) :
        """
        return a string array with general stats about this run
        """

        msg = [ "%s\t%s" % ("pyFlowClientWorkflowClass:", self.param.workflowClassName),
              "%s\t%s" % ("pyFlowVersion:", __version__),
              "%s\t%s" % ("pythonVersion:", pythonVersion),
              "%s\t%s" % ("Runid:", self.getRunid()),
              "%s\t%s UTC" % ("RunStartTime:", self.param.logRunStartTime),
              "%s\t%s UTC" % ("NotificationTime:", timeStrNow()),
              "%s\t%s" % ("HostName:", siteConfig.getHostName()),
              "%s\t%s" % ("WorkingDir:", self.cwd),
              "%s\t%s" % ("DataDir:", self.param.dataDir),
              "%s\t'%s'" % ("ProcessCmdLine:", cmdline()) ]
        return msg


    def emailNotification(self, msgList, emailErrorLog=None) :
        #
        # email addy might not be setup yet:
        #
        # if errorLog is specified, then an email send exception will
        # be handled and logged, otherwise the exception will be re-raised
        # down to the caller.
        #

        if self.param is None : return
        if len(self.param.mailTo) == 0 : return

        if not isLocalSmtp() :
            if emailErrorLog :
                msg = ["email notification failed, no local smtp server"]
                emailErrorLog(msg,logState=LogState.WARNING)
            return

        mailTo = sorted(list(self.param.mailTo))
        subject = "pyflow notification from %s run: %s" % (self.param.workflowClassName, self.getRunid())
        msg = msgListToMsg(msgList)
        fullMsgList = ["Message:",
                      '"""',
                      msg,
                      '"""']
        fullMsgList.extend(self.getInfoMsg())

        import smtplib
        try:
            sendEmail(mailTo, siteConfig.mailFrom, subject, fullMsgList)
        except smtplib.SMTPException :
            if emailErrorLog is None : raise
            msg = ["email notification failed"]
            eMsg = lister(getExceptionMsg())
            msg.extend(eMsg)
            emailErrorLog(msg,logState=LogState.WARNING)



class WorkflowRunner(object) :
    """
    This object is designed to be inherited by a class in
    client code. This inheriting class can override the
    L{workflow()<WorkflowRunner.workflow>} method to define the
    tasks that need to be run and their dependencies.

    The inheriting class defining a workflow can be executed in
    client code by calling the WorkflowRunner.run() method.
    This method provides various run options such as whether
    to run locally or on sge.
    """


    _maxWorkflowRecursion = 30
    """
    This limit protects against a runaway forkbomb in case a
    workflow task recursively adds itself w/o termination:
    """


    def run(self,
            mode="local",
            dataDirRoot=".",
            isContinue=False,
            isForceContinue=False,
            nCores=None,
            memMb=None,
            isDryRun=False,
            retryMax=2,
            retryWait=90,
            retryWindow=360,
            retryMode="nonlocal",
            mailTo=None,
            updateInterval=60,
            schedulerArgList=None,
            isQuiet=False,
            warningLogFile=None,
            errorLogFile=None,
            successMsg=None,
            startFromTasks=None,
            ignoreTasksAfter=None,
            resetTasks=None) :
        """
        Call this method to execute the workflow() method overridden
        in a child class and specify the resources available for the
        workflow to run.

        Task retry behavior: Retry attempts will be made per the
        arguments below for distributed workflow runs (eg. sge run
        mode). Note this means that retries will be attempted for
        tasks with an 'isForceLocal' setting during distributed runs.

        Task error behavior: When a task error occurs the task
        manager stops submitting new tasks and allows all currently
        running tasks to complete. Note that in this case 'task error'
        means that the task could not be completed after exhausting
        attempted retries.

        Workflow exception behavior: Any exceptions thrown from the
        python code of classes derived from WorkflowRunner will be
        logged and trigger notification (e.g. email). The exception
        will not come down to the client's stack. In sub-workflows the
        exception is handled exactly like a task error (ie. task
        submission is shut-down and remaining tasks are allowed to
        complete). An exception in the master workflow will lead to
        workflow termination without waiting for currently running
        tasks to finish.

        @return: 0 if all tasks completed successfully and 1 otherwise

        @param mode: Workflow run mode. Current options are (local|sge)

        @param dataDirRoot: All workflow data is written to
                       {dataDirRoot}/pyflow.data/ These include
                       workflow/task logs, persistent task state data,
                       and summary run info. Two workflows cannot
                       simultaneously use the same dataDir.

        @param isContinue: If True, continue workflow from a previous
                      incomplete run based on the workflow data
                      files. You must use the same dataDirRoot as a
                      previous run for this to work.  Set to 'Auto' to
                      have the run continue only if the previous
                      dataDir exists.  (default: False)

        @param isForceContinue: Only used if isContinue is not False. Normally
                           when isContinue is run, the commands of
                           completed tasks are checked to ensure they
                           match. When isForceContinue is true,
                           failing this check is reduced from an error
                           to a warning

        @param nCores: Total number of cores available, or 'unlimited', sge
                  is currently configured for a maximum job count of
                  %s, any value higher than this in sge mode will be
                  reduced to the maximum.  (default: 1 for local mode,
                  %s for sge mode)

        @param memMb: Total memory available (in megabytes), or 'unlimited',
                 Note that this value will be ignored in non-local modes
                 (such as sge), because in this case total memory available
                 is expected to be known by the scheduler for each node in its
                 cluster. (default: %i*nCores for local mode, 'unlimited'
                 for sge mode)

        @param isDryRun: List the commands to be executed without running
                    them. Note that recursive and dynamic workflows
                    will potentially have to account for the fact that
                    expected files will be missing -- here 'recursive
                    workflow' refers to any workflow which uses the
                    addWorkflowTask() method, and 'dynamic workflow'
                    refers to any workflow which uses the
                    waitForTasks() method. These types of workflows
                    can query this status with the isDryRun() to make
                    accomadations.  (default: False)

        @param retryMax: Maximum number of task retries

        @param retryWait: Delay (in seconds) before resubmitting task

        @param retryWindow: Maximum time (in seconds) after the first task
                       submission in which retries are allowed. A value of
                       zero or less puts no limit on the time when retries
                       will be attempted. Retries are always allowed (up to
                       retryMax times), for failed make jobs.

        @param retryMode: Modes are 'nonlocal' and 'all'. For 'nonlocal'
                retries are not attempted in local run mode. For 'all'
                retries are attempted for any run mode. The default mode
                is 'nonolocal'.

        @param mailTo: An email address or container of email addresses. Notification
                  will be sent to each email address when
                  either (1) the run successfully completes (2) the
                  first task error occurs or (3) an unhandled
                  exception is raised. The intention is to send one
                  status message per run() indicating either success
                  or the reason for failure. This should occur for all
                  cases except a host hardware/power failure.  Note
                  that mail comes from '%s' (configurable),
                  which may be classified as junk-mail by your system.

        @param updateInterval: How often (in minutes) should pyflow log a
                          status update message summarizing the run
                          status.  Set this to zero or less to turn
                          the update off.

        @param schedulerArgList: A list of arguments can be specified to be
                            passed on to an external scheduler when non-local
                            modes are used (e.g. in sge mode you could pass
                            schedulerArgList=['-q','work.q'] to put the whole
                            pyflow job into the sge work.q queue)

        @param isQuiet: Don't write any logging output to stderr (but still write
                   log to pyflow_log.txt)

        @param warningLogFile: Replicate all warning messages to the specified file. Warning
                            messages will still appear in the standard logs, this
                            file will contain a subset of the log messages pertaining to
                            warnings only.

        @param errorLogFile: Replicate all error messages to the specified file. Error
                            messages will still appear in the standard logs, this
                            file will contain a subset of the log messages pertaining to
                            errors only. It should be empty for a successful run.

        @param successMsg: Provide a string containing a custom message which
                           will be prepended to pyflow's standard success
                           notification. This message will appear in the log
                           and any configured notifications (e.g. email). The
                           message may contain linebreaks.

        @param startFromTasks: A task label or container of task labels. Any tasks which
                               are not in this set or descendants of this set will be marked as
                               completed.
        @type startFromTasks: A single string, or set, tuple or list of strings

        @param ignoreTasksAfter: A task label or container of task labels. All descendants
                                 of these task labels will be ignored.
        @type ignoreTasksAfter: A single string, or set, tuple or list of strings

        @param resetTasks: A task label or container of task labels. These tasks and all
                           of their descendants will be reset to the "waiting" state to be re-run.
                           Note this option will only affect a workflow which has been continued
                           from a previous run. This will not override any nodes altered by the
                           startFromTasks setting in the case that both options are used together.
        @type resetTasks: A single string, or set, tuple or list of strings
        """

        # Setup pyflow signal handlers:
        #
        inHandlers = Bunch(isSet=False)

        class SigTermException(Exception) : pass

        def sigtermHandler(_signum, _frame) :
            raise SigTermException

        def sighupHandler(_signum, _frame) :
            self._warningLog("pyflow recieved hangup signal. pyflow will continue, but this signal may still interrupt running tasks.")
            # tell cdata to turn off any tty writes:
            self._cdata().isHangUp.set()

        def set_pyflow_sig_handlers() :
            import signal
            if not inHandlers.isSet :
                inHandlers.sigterm = signal.getsignal(signal.SIGTERM)
                if not isWindows() :
                    inHandlers.sighup = signal.getsignal(signal.SIGHUP)
                inHandlers.isSet = True
            try:
                signal.signal(signal.SIGTERM, sigtermHandler)
                if not isWindows() :
                    signal.signal(signal.SIGHUP, sighupHandler)
            except ValueError:
                if isMainThread() :
                    raise
                else :
                    self._warningLog("pyflow has not been initialized on main thread, all custom signal handling disabled")


        def unset_pyflow_sig_handlers() :
            import signal
            if not inHandlers.isSet : return
            try :
                signal.signal(signal.SIGTERM, inHandlers.sigterm)
                if not isWindows() :
                    signal.signal(signal.SIGHUP, inHandlers.sighup)
            except ValueError:
                if isMainThread() :
                    raise
                else:
                    pass


        # if return value is somehow not set after this then something bad happened, so init to 1:
        retval = 1
        try:
            set_pyflow_sig_handlers()

            def exceptionMessaging(prefixMsg=None) :
                msg = lister(prefixMsg)
                eMsg = lister(getExceptionMsg())
                msg.extend(eMsg)
                self._notify(msg,logState=LogState.ERROR)

            try:
                self.runStartTimeStamp = time.time()
                self.updateInterval = int(updateInterval)
                # a container to haul all the run() options around in:
                param = Bunch(mode=mode,
                              dataDir=dataDirRoot,
                              isContinue=isContinue,
                              isForceContinue=isForceContinue,
                              nCores=nCores,
                              memMb=memMb,
                              isDryRun=isDryRun,
                              retryMax=retryMax,
                              retryWait=retryWait,
                              retryWindow=retryWindow,
                              retryMode=retryMode,
                              mailTo=mailTo,
                              logRunStartTime=timeStampToTimeStr(self.runStartTimeStamp),
                              workflowClassName=self._whoami(),
                              schedulerArgList=schedulerArgList,
                              isQuiet=isQuiet,
                              warningLogFile=warningLogFile,
                              errorLogFile=errorLogFile,
                              successMsg=successMsg,
                              startFromTasks=setzer(startFromTasks),
                              ignoreTasksAfter=setzer(ignoreTasksAfter),
                              resetTasks=setzer(resetTasks))
                retval = self._runWorkflow(param)

            except SigTermException:
                msg = "Received termination signal, shutting down running tasks..."
                self._killWorkflow(msg)
            except KeyboardInterrupt:
                msg = "Keyboard Interrupt, shutting down running tasks..."
                self._killWorkflow(msg)
            except DataDirException, e:
                self._notify(e.msg,logState=LogState.ERROR)
            except:
                exceptionMessaging()
                raise

        finally:
            # last set: disconnect the workflow log:
            self._cdata().resetRun()
            unset_pyflow_sig_handlers()

        return retval


    # configurable elements of docstring
    run.__doc__ = run.__doc__ % (siteConfig.maxSGEJobs,
                                 RunMode.data["sge"].defaultCores,
                                 siteConfig.defaultHostMemMbPerCore,
                                 siteConfig.mailFrom)



    # protected methods which can be called within the workflow method:

    def addTask(self, label, command=None, cwd=None, env=None, nCores=1,
                memMb=siteConfig.defaultTaskMemMb,
                dependencies=None, priority=0,
                isForceLocal=False, isCommandMakePath=False, isTaskStable=True,
                mutex=None,
                retryMax=None, retryWait=None, retryWindow=None, retryMode=None) :
        """
        Add task to workflow, including resource requirements and
        specification of dependencies. Dependency tasks must already
        exist in the workflow.

        @return: The 'label' argument is returned without modification.


        @param label: A string used to identify each task. The label must
                 be composed of only ascii letters, digits,
                 underscores and dashes (ie. /[A-Za-z0-9_-]+/). The
                 label must also be unique within the workflow, and
                 non-empty.

        @param command: The task command. Commands can be: (1) a shell
                 string (2) an iterable container of strings (argument
                 list) (3) None. In all cases strings must not contain
                 newline characters. A single string is typically used
                 for commands that require shell features (such as
                 pipes), an argument list can be used for any other
                 commands, this is often a useful way to simplify
                 quoting issues or to submit extremely long
                 commands. The default command (None), can be used to
                 create a 'checkpoint', ie. a task which does not run
                 anything, but provides a label associated with the
                 completion of a set of dependencies.

        @param cwd: Specify current working directory to use for
                 command execution. Note that if submitting the
                 command as an argument list (as opposed to a shell
                 string) the executable (arg[0]) is searched for
                 before changing the working directory, so you cannot
                 specify the executable relative to the cwd
                 setting. If submitting a shell string command this
                 restriction does not apply.

        @param env: A map of environment variables for this task, for
                 example 'env={"PATH": "/usr/bin"}'. When env is set
                 to None (the default) the environment of the pyflow
                 client process is used.

        @param nCores: Number of cpu threads required

        @param memMb: Amount of memory required (in megabytes)

        @param dependencies: A task label or container of task labels specifying all dependent
                        tasks. Dependent tasks must already exist in
                        the workflow.
        @type dependencies: A single string, or set, tuple or list of strings


        @param priority: Among all tasks which are eligible to run at
                 the same time, launch tasks with higher priority
                 first. this value can be set from[-100,100]. Note
                 that this will strongly control the order of task
                 launch on a local run, but will only control task
                 submission order to a secondary scheduler (like
                 sge). All jobs with the same priority are already
                 submitted in order from highest to lowest nCores
                 requested, so there is no need to set priorities to
                 replicate this behavior. The taskManager can start
                 executing tasks as soon as each addTask() method is
                 called, so lower-priority tasks may be launched first
                 if they are specified first in the workflow.

        @param isForceLocal: Force this task to run locally when a
	         distributed task mode is used. This can be used to
	         launch very small jobs outside of the sge queue. Note
	         that 'isForceLocal' jobs launched during a non-local
	         task mode are not subject to resource management, so
	         it is important that these represent small
	         jobs. Tasks which delete, move or touch a small
	         number of files are ideal for this setting.

        @param isCommandMakePath: If true, command is assumed to be a
                 path containing a makefile. It will be run using
                 make/qmake according to the run's mode and the task's
                 isForceLocal setting

        @param isTaskStable: If false, indicates that the task command
                 and/or dependencies may change if the run is
                 interrupted and restarted. A command marked as
                 unstable will not be checked to make sure it matches
                 its previous definition during run continuation.
                 Unstable examples: command contains a date/time, or
                 lists a set of files which are deleted at some point
                 in the workflow, etc.

        @param mutex: Provide an optional id associated with a pyflow
                 task mutex. For all tasks with the same mutex id, no more
                 than one will be run at once. Id name must follow task id
                 restrictions. Mutex ids are global across all recursively
                 invoked workflows.
                 Example use case: This feature has been added as a simpler
                 alternative to file locking, to ensure sequential, but not
                 ordered, access to a file.

        @param retryMax: The number of times this task will be retried
                 after failing. If defined, this overrides the workflow
                 retryMax value.

        @param retryWait: The number of seconds to wait before relaunching
                 a failed task. If defined, this overrides the workflow
                 retryWait value.

        @param retryWindow: The number of seconds after job submission in
                 which retries will be attempted for non-make jobs. A value of
                 zero or less causes retries to be attempted anytime after
                 job submission. If defined, this overrides the workflow
                 retryWindow value.

        @param retryMode: Modes are 'nonlocal' and 'all'. For 'nonlocal'
                retries are not attempted in local run mode. For 'all'
                retries are attempted for any run mode. If defined, this overrides
                the workflow retryMode value.
        """

        self._requireInWorkflow()

        #### Canceled plans to add deferred dependencies:
        # # deferredDependencies -- A container of labels specifying dependent
        # #                         tasks which have not yet been added to the
        # #                         workflow. In this case the added task will
        # #                         wait for the dependency to be defined *and*
        # #                         complete. Avoid these in favor or regular
        # #                         dependencies if possible.

        # sanitize bools:
        isForceLocal = argToBool(isForceLocal)
        isCommandMakePath = argToBool(isCommandMakePath)

        # sanitize ints:
        nCores = int(nCores)
        memMb = int(memMb)
        priority = int(priority)
        if (priority > 100) or (priority < -100) :
            raise Exception("priority must be an integer in the range [-100,100]")

        # sanity check label:
        WorkflowRunner._checkTaskLabel(label)

        fullLabel = namespaceJoin(self._getNamespace(), label)

        # verify/sanitize command:
        cmd = Command(command, cwd, env)

        # deal with command/resource relationship:
        if cmd.cmd is None :
            nCores = 0
            memMb = 0
        else:
            if nCores <= 0 :
                raise Exception("Unexpected core requirement for task: '%s' nCores: %i" % (fullLabel, nCores))
            if memMb <= 0:
                raise Exception("Unexpected memory requirement for task: '%s' memory: %i (megabytes)" % (fullLabel, memMb))


        if (self._cdata().param.nCores != "unlimited") and (nCores > self._cdata().param.nCores) :
            raise Exception("Task core requirement exceeds full available resources")

        if (self._cdata().param.memMb != "unlimited") and (memMb > self._cdata().param.memMb) :
            raise Exception("Task memory requirement exceeds full available resources")

        # check that make path commands point to a directory:
        #
        if isCommandMakePath :
            if cmd.type != "str" :
                raise Exception("isCommandMakePath is set, but no path is provided in task: '%s'" % (fullLabel))
            cmd.cmd = os.path.abspath(cmd.cmd)

        # sanitize mutex option
        if mutex is not None :
            WorkflowRunner._checkTaskLabel(mutex)

        task_retry = self._cdata().param.retry.getTaskCopy(retryMax, retryWait, retryWindow, retryMode)

        # private _addTaskCore gets hijacked in recursive workflow submission:
        #
        payload = CmdPayload(fullLabel, cmd, nCores, memMb, priority, isForceLocal, isCommandMakePath, isTaskStable, mutex, task_retry)
        self._addTaskCore(self._getNamespace(), label, payload, dependencies)
        return label




    def addWorkflowTask(self, label, workflowRunnerInstance, dependencies=None) :
        """
        Add another WorkflowRunner instance as a task to this
        workflow. The added Workflow's workflow() method will be
        called once the dependencies specified in this call have
        completed. Once started, all of the submitted workflow's
        method calls (like addTask) will be placed into the enclosing
        workflow instance and bound by the run parameters of the
        enclosing workflow.

        This task will be marked complete once the submitted workflow's
        workflow() method has finished, and any tasks it initiated have
        completed.

        Note that all workflow tasks will have their own tasks namespaced
        with the workflow task label. This namespace is recursive in the
        case that you add workflow tasks which add their own workflow
        tasks, etc.

        Note that the submitted workflow instance will be deep copied
        before being altered in any way.

        @return: The 'label' argument is returned without modification.

        @param label: A string used to identify each task. The label must
                 be composed of only ascii letters, digits,
                 underscores and dashes (ie. /[A-Za-z0-9_-]+/). The
                 label must also be unique within the workflow, and
                 non-empty.

        @param workflowRunnerInstance: A L{WorkflowRunner} instance.

        @param dependencies: A label string or container of labels specifying all dependent
                        tasks. Dependent tasks must already exist in
                        the workflow.
        @type dependencies: A single string, or set, tuple or list of strings
        """

        self._requireInWorkflow()

        # sanity check label:
        WorkflowRunner._checkTaskLabel(label)

        import inspect

        # copy and 'hijack' the workflow:
        workflowCopy = copy.deepcopy(workflowRunnerInstance)

        # hijack! -- take all public methods at the WorkflowRunner level
        # (except workflow()), and insert the self copy:
        publicExclude = ["workflow", "addTask", "addWorkflowTask", "waitForTasks"]
        for (n, _v) in inspect.getmembers(WorkflowRunner, predicate=inspect.ismethod) :
            if n[0] == "_" : continue  # skip private/special methods
            if n in publicExclude : continue
            setattr(workflowCopy, n, getattr(self, n))

        privateInclude = ["_cdata", "_addTaskCore", "_waitForTasksCore", "_isTaskCompleteCore","_setRunning","_getRunning"]
        for n in privateInclude :
            setattr(workflowCopy, n, getattr(self, n))

        # final step: disable the run() function to be extra safe...
        workflowCopy.run = None

        # set the task namespace:
        workflowCopy._appendNamespace(self._getNamespaceList())
        workflowCopy._appendNamespace(label)

        # add workflow task to the task-dag, and launch a new taskrunner thread
        # if one isn't already running:
        payload = WorkflowPayload(workflowCopy)
        self._addTaskCore(self._getNamespace(), label, payload, dependencies)
        return label


    def waitForTasks(self, labels=None) :
        """
        Wait for a list of tasks to complete.

        @return: In case of an error in a task being waited for, or in
                 one of these task's dependencies, the function returns 1.
                 Else return 0.

        @param labels: Container of task labels to wait for. If an empty container is
                  given or no list is provided then wait for all
                  outstanding tasks to complete.
        @type labels: A single string, or set, tuple or list of strings
        """

        self._requireInWorkflow()

        return self._waitForTasksCore(self._getNamespace(), labels)


    def isTaskComplete(self, taskLabel) :
        """
        Query if a specific task is in the workflow and completed without error.

        This can assist workflows with providing
        stable interrupt/resume behavior.

        @param taskLabel: A task string

        @return: Completion status of task
        """

        return self._isTaskCompleteCore(self._getNamespace(), taskLabel)


    def getRunMode(self) :
        """
        Get the current run mode

        This can be used to access the current run mode from
        within the workflow function. Although the runmode should
        be transparent to client code, this is occasionally needed
        to hack workarounds.

        @return: Current run mode
        """

        self._requireInWorkflow()

        return self._cdata().param.mode


    def getNCores(self) :
        """
        Get the current run core limit

        This function can be used to access the current run's core
        limit from within the workflow function. This can be useful
        to eg. limit the number of cores requested by a single task.

        @return: Total cores available to this workflow run
        @rtype: Integer value or 'unlimited'
        """

        self._requireInWorkflow()

        return self._cdata().param.nCores


    def limitNCores(self, nCores) :
        """
        Takes an task nCores argument and reduces it to
        the maximum value allowed for the current run.

        @param nCores: Proposed core requirement

        @return: Min(nCores,Total cores available to this workflow run)
        """

        self._requireInWorkflow()

        nCores = int(nCores)
        runNCores = self._cdata().param.nCores
        if runNCores == "unlimited" : return nCores
        return min(nCores, runNCores)


    def getMemMb(self) :
        """
        Get the current run's total memory limit (in megabytes)

        @return: Memory limit in megabytes
        @rtype: Integer value or 'unlimited'
        """

        self._requireInWorkflow()

        return self._cdata().param.memMb


    def limitMemMb(self, memMb) :
        """
        Takes a task memMb argument and reduces it to
        the maximum value allowed for the current run.

        @param memMb: Proposed task memory requirement in megabytes

        @return: Min(memMb,Total memory available to this workflow run)
        """

        self._requireInWorkflow()

        memMb = int(memMb)
        runMemMb = self._cdata().param.memMb
        if runMemMb == "unlimited" : return memMb
        return min(memMb, runMemMb)


    def isDryRun(self) :
        """
        Get isDryRun flag value.

        When the dryrun flag is set, no commands are actually run. Querying
        this flag allows dynamic workflows to correct for dry run behaviors,
        such as tasks which do no produce expected files.

        @return: DryRun status flag
        """

        self._requireInWorkflow()

        return self._cdata().param.isDryRun


    @staticmethod
    def runModeDefaultCores(mode) :
        """
        Get the default core limit for run mode (local,sge,..)

        @param mode: run mode, as specified in L{the run() method<WorkflowRunner.run>}

        @return: Default maximum number of cores for mode

        @rtype: Either 'unlimited', or a string
                 representation of the integer limit
        """

        return str(RunMode.data[mode].defaultCores)


    def flowLog(self, msg, logState = LogState.INFO) :
        """
        Send a message to the WorkflowRunner's log.

        @param msg: Log message
        @type msg: A string or an array of strings. String arrays will be separated by newlines in the log.
        @param logState: Message severity, defaults to INFO.
        @type logState: A value in pyflow.LogState.{INFO,WARNING,ERROR}
        """

        self._requireInWorkflow()

        linePrefixOut = "[%s]" % (self._cdata().param.workflowClassName)
        self._cdata().flowLog(msg, linePrefix=linePrefixOut, logState=logState)


    # Protected methods for client derived-class override:

    def workflow(self) :
        """
        Workflow definition defined in child class

        This method should be overridden in the class derived from
        L{WorkflowRunner} to specify the actual workflow logic. Client
        code should not call this method directly.
        """
        pass


    # private methods:

    # special workflowRunner Exception used to terminate workflow() function
    # if a ctrl-c is issued
    class _AbortWorkflowException(Exception) :
        pass


    def _flowLog(self, msg, logState) :
        linePrefixOut = "[WorkflowRunner]"
        self._cdata().flowLog(msg, linePrefix=linePrefixOut, logState=logState)

    def _infoLog(self, msg) :
        self._flowLog(msg,logState=LogState.INFO)

    def _warningLog(self, msg) :
        self._flowLog(msg,logState=LogState.WARNING)

    def _errorLog(self, msg) :
        self._flowLog(msg,logState=LogState.ERROR)

    def _whoami(self) :
        # returns name of *derived* class
        return self.__class__.__name__


    def _getNamespaceList(self) :
        try:
            return self._namespaceList
        except AttributeError:
            self._namespaceList = []
            return self._namespaceList

    def _getNamespace(self) :
        return namespaceSep.join(self._getNamespaceList())

    def _appendNamespace(self, names) :
        names = lister(names)
        for name in names :
            # check against runaway recursion:
            if len(self._getNamespaceList()) >= WorkflowRunner._maxWorkflowRecursion :
                raise Exception("Recursive workflow invocation depth exceeds maximum allowed depth of %i" % (WorkflowRunner._maxWorkflowRecursion))
            WorkflowRunner._checkTaskLabel(name)
            self._getNamespaceList().append(name)


    # flag used to request the termination of all task submission:
    #
    _allStop = threading.Event()

    @staticmethod
    def _stopAllWorkflows() :
        # request all workflows stop task submission:
        WorkflowRunner._allStop.set()

    @staticmethod
    def _isWorkflowStopped() :
        # check whether a global signal has been give to stop all workflow submission
        # this should only be true when a ctrl-C or similar event has occurred.
        return WorkflowRunner._allStop.isSet()

    def _addTaskCore(self, namespace, label, payload, dependencies) :
        # private core taskAdd routine for hijacking
        # fromWorkflow is the workflow instance used to launch the task
        #

        # add workflow task to the task-dag, and launch a new taskrunner thread
        # if one isn't already running:
        if self._isWorkflowStopped() :
            raise WorkflowRunner._AbortWorkflowException

        self._infoLog("Adding %s '%s' to %s" % (payload.desc(), namespaceJoin(namespace, label), namespaceLabel(namespace)))

        # add task to the task-dag, and launch a new taskrunner thread
        # if one isn't already running:
        dependencies = setzer(dependencies)
        self._tdag.addTask(namespace, label, payload, dependencies)
        self._startTaskManager()


    def _getWaitStatus(self, namespace, labels, status) :
        # update and return two values:
        # (1) isAllTaskDone -- are all tasks done (ie. error or complete state
        # (2) retval -- this is set to one if any tasks have errors
        #

        def updateStatusFromTask(task, status) :
            if not task.isDone() :
                status.isAllTaskDone = False
            elif not task.isComplete() :
                status.retval = 1
            if status.retval == 0 and (not self._cdata().isTaskSubmissionActive()) :
                status.retval = 1
            if status.retval == 0 and task.isDead() :
                status.retval = 1


        if len(labels) == 0 :
            if namespace == "" :
                if self._tdag.isRunExhausted() or (not self._tman.isAlive()) :
                    if not self._tdag.isRunComplete() :
                        status.retval = 1
                else:
                    status.isAllTaskDone = False
            else :
                for task in self._tdag.getAllNodes(namespace) :
                    updateStatusFromTask(task, status)
        else :
            for l in labels :
                if not self._tdag.isTaskPresent(namespace, l) :
                    raise Exception("Task: '%s' is not in taskDAG" % (namespaceJoin(namespace, l)))
                task = self._tdag.getTask(namespace, l)
                updateStatusFromTask(task, status)


    def _waitForTasksCore(self, namespace, labels=None, isVerbose=True) :
        labels = setzer(labels)
        if isVerbose :
            msg = "Pausing %s until completion of" % (namespaceLabel(namespace))
            if len(labels) == 0 :
                self._infoLog(msg + " its current tasks")
            else:
                self._infoLog(msg + " task(s): %s" % (",".join([namespaceJoin(namespace, l) for l in labels])))

        class WaitStatus:
            def __init__(self) :
                self.isAllTaskDone = True
                self.retval = 0

        ewaiter = ExpWaiter(1, 1.7, 15)
        while True :
            if self._isWorkflowStopped() :
                raise WorkflowRunner._AbortWorkflowException
            status = WaitStatus()
            self._getWaitStatus(namespace, labels, status)
            if status.isAllTaskDone or (status.retval != 0) : break
            ewaiter.wait()

        if isVerbose :
            msg = "Resuming %s" % (namespaceLabel(namespace))
            self._infoLog(msg)
        return status.retval


    def _isTaskCompleteCore(self, namespace, taskLabel) :

        if not self._tdag.isTaskPresent(namespace, taskLabel) :
            return False
        task = self._tdag.getTask(namespace, taskLabel)
        return task.isComplete()


    @staticmethod
    def _checkTaskLabel(label) :
        # sanity check label:
        if not isinstance(label, basestring) :
            raise Exception ("Task label is not a string")
        if label == "" :
            raise Exception ("Task label is empty")
        if not re.match("^[A-Za-z0-9_-]+$", label) :
            raise Exception ("Task label is invalid due to disallowed characters. Label: '%s'" % (label))


    def _startTaskManager(self) :
        # start a new task manager if one isn't already running:
        #
        if (self._tman is not None) and (self._tman.isAlive()) : return
        if not self._cdata().isTaskManagerException :
            self._tman = TaskManager(self._cdata(), self._tdag)
            self._tman.start()


    def _notify(self, msg, logState) :
        # msg is printed to log AND sent to any email or other requested
        # notification systems:
        self._flowLog(msg,logState)
        self._cdata().emailNotification(msg, self._flowLog)


    def _killWorkflow(self, errorMsg) :
        self._notify(errorMsg,logState=LogState.ERROR)
        self._shutdownAll(timeoutSec=10)
        sys.exit(1)


    def _shutdownAll(self, timeoutSec) :
        # Try to shut down the task manager, all command-tasks,
        # and all sub-workflow tasks.
        #
        if (self._tman is None) or (not self._tman.isAlive()) : return
        StoppableThread.stopAll()
        self._stopAllWorkflows()
        self._tman.stop()
        for _ in range(timeoutSec) :
            time.sleep(1)
            if not self._tman.isAlive() :
                self._infoLog("Task shutdown complete")
                return
        self._infoLog("Task shutdown timed out")


    def _cdata(self) :
        # We're doing this convoluted setup only to avoid having a
        # ctor for ease of use by the client. See what pyFlow goes
        # through for you client code??
        #
        try:
            return self._constantData
        except AttributeError:
            self._constantData = WorkflowRunnerThreadSharedData()
            return self._constantData


    # TODO: Better definition of the status thread shutdown at the end of a pyflow run to
    # prevent race conditions -- ie. what happens if the status update is running while
    # pyflow is shutting down? Every method called by the status updater should be safety
    # checked wrt this issue.
    #
    def _runUpdate(self, runStatus) :
        while True :
            time.sleep(self.updateInterval * 60)

            status = self._tdag.getTaskStatus()
            isSpecComplete = (runStatus.isSpecificationComplete.isSet() and status.isAllSpecComplete)
            report = []
            report.append("===== " + self._whoami() + " StatusUpdate =====")
            report.append("Workflow specification is complete?: %s" % (str(isSpecComplete)))
            report.append("Task status (waiting/queued/running/complete/error): %i/%i/%i/%i/%i"
                          % (status.waiting, status.queued, status.running, status.complete, status.error))
            report.append("Longest ongoing queued task time (hrs): %.4f" % (status.longestQueueSec / 3600.))
            report.append("Longest ongoing queued task name: '%s'" % (status.longestQueueName))
            report.append("Longest ongoing running task time (hrs): %.4f" % (status.longestRunSec / 3600.))
            report.append("Longest ongoing running task name: '%s'" % (status.longestRunName))

            report = [ "[StatusUpdate] " + line for line in report ]
            self._infoLog(report)

            # Update interval is also an appropriate interval to dump a stack-trace of all active
            # threads. This is a useful post-mortem in the event of a large class of hang/deadlock
            # errors:
            #
            stackDumpFp = open(self._cdata().stackDumpLogFile, "a")

            # create one fully decorated line in the stack dump file as a prefix to the report:
            linePrefixOut = "[%s] [StackDump]" % (self._cdata().getRunid())
            ofpList = [stackDumpFp]
            log(ofpList, "Initiating stack dump for all threads", linePrefixOut)

            stackDump(stackDumpFp)
            hardFlush(stackDumpFp)
            stackDumpFp.close()



    def _runWorkflow(self, param) :
        #
        # Primary workflow logic when nothing goes wrong:
        #
        self._setupWorkflow(param)
        self._initMessage()

        runStatus = RunningTaskStatus(self._tdag.isFinishedEvent)

        # start status update reporter:
        #
        # TODO: stop this thread at end of run
        #
        if(self.updateInterval > 0) :
            hb = threading.Thread(target=WorkflowRunner._runUpdate, args=(self, runStatus))
            hb.setDaemon(True)
            hb.setName("StatusUpdate-Thread")
            hb.start()

        # run workflow() function on a separate thread, using exactly
        # the same method we use for sub-workflows:
        #
        # TODO: move the master workflow further into the code path used by sub-workflows,
        # so that we aren't replicating polling and error handling code in this function:
        #
        trun = WorkflowTaskRunner(runStatus, "masterWorkflow", self, self._cdata().flowLog, None)
        trun.start()
        # can't join() because that blocks SIGINT
        ewaiter = ExpWaiter(1, 1.7, 15,runStatus.isComplete)
        while True :
            if not trun.isAlive() : break
            ewaiter.wait()

        if not runStatus.isComplete.isSet() :
            # if not complete then we don't know what happened, very bad!:
            runStatus.errorCode = 1
            runStatus.errorMessage = "Thread: '%s', has stopped without a traceable cause" % (trun.getName())

        return self._evalWorkflow(runStatus)


    def _setupWorkflow(self, param) :
        cdata = self._cdata()

        # setup instance user parameters:
        cdata.setupNewRun(param)

        # setup other instance data:
        self._tdag = TaskDAG(cdata.param.isContinue, cdata.param.isForceContinue, cdata.param.isDryRun,
                             cdata.taskInfoFile, cdata.taskStateFile, cdata.param.workflowClassName,
                             cdata.param.startFromTasks, cdata.param.ignoreTasksAfter, cdata.param.resetTasks,
                             self._flowLog)
        self._tman = None

        def backupFile(inputFile) :
            """
            backup old state files if they exist
            """
            if not os.path.isfile(inputFile) : return
            fileDir = os.path.dirname(inputFile)
            fileName = os.path.basename(inputFile)
            backupDir = os.path.join(fileDir, "backup")
            ensureDir(backupDir)
            backupFileName = fileName + ".backup_before_starting_run_%s.txt" % (cdata.getRunid())
            backupFile = os.path.join(backupDir, backupFileName)
            shutil.copyfile(inputFile, backupFile)

        backupFile(cdata.taskStateFile)
        backupFile(cdata.taskInfoFile)

        if cdata.param.isContinue :
            self._setupContinuedWorkflow()



    def _createContinuedStateFile(self) :
        #
        # create continued version of task state file
        #

        cdata = self._cdata()
        if not os.path.isfile(cdata.taskStateFile) : return set()

        tmpFile = cdata.taskStateFile + ".update.incomplete"
        tmpfp = open(tmpFile, "w")
        tmpfp.write(taskStateHeader())
        complete = set()
        for words in taskStateParser(cdata.taskStateFile) :
            (runState, errorCode) = words[2:4]
            if (runState != "complete") or (int(errorCode) != 0) : continue
            tmpfp.write("\t".join(words) + "\n")
            (label, namespace) = words[0:2]
            complete.add(namespaceJoin(namespace, label))

        tmpfp.close()
        forceRename(tmpFile, cdata.taskStateFile)
        return complete


    def _createContinuedInfoFile(self, complete) :
        #
        # create continued version of task info file
        #

        cdata = self._cdata()
        if not os.path.isfile(cdata.taskInfoFile) : return

        tmpFile = cdata.taskInfoFile + ".update.incomplete"
        tmpfp = open(tmpFile, "w")
        tmpfp.write(taskInfoHeader())
        for words in taskInfoParser(cdata.taskInfoFile) :
            (label, namespace, ptype, nCores, memMb, priority, isForceLocal, depStr, cwdStr, command) = words
            fullLabel = namespaceJoin(namespace, label)
            if fullLabel not in complete : continue
            tmpfp.write("\t".join(words) + "\n")
            if   ptype == "command" :
                if command == "" : command = None
                payload = CmdPayload(fullLabel, Command(command, cwdStr), int(nCores), int(memMb), int(priority), argToBool(isForceLocal))
            elif ptype == "workflow" :
                payload = WorkflowPayload(None)
            else : assert 0

            self._tdag.addTask(namespace, label, payload, getTaskInfoDepSet(depStr), isContinued=True)

        tmpfp.close()
        forceRename(tmpFile, cdata.taskInfoFile)



    def _setupContinuedWorkflow(self) :
        # reduce both state files to completed states only.
        complete = self._createContinuedStateFile()
        self._createContinuedInfoFile(complete)



    def _initMessage(self) :
        param = self._cdata().param  # shortcut
        msg = ["Initiating pyFlow run"]
        msg.append("pyFlowClientWorkflowClass: %s" % (param.workflowClassName))
        msg.append("pyFlowVersion: %s" % (__version__))
        msg.append("pythonVersion: %s" % (pythonVersion))
        msg.append("WorkingDir: '%s'" % (self._cdata().cwd))
        msg.append("ProcessCmdLine: '%s'" % (cmdline()))

        parammsg = ["mode: %s" % (param.mode),
                  "nCores: %s" % (str(param.nCores)),
                  "memMb: %s" % (str(param.memMb)),
                  "dataDir: %s" % (str(param.dataDir)),
                  "isDryRun: %s" % (str(param.isDryRun)),
                  "isContinue: %s" % (str(param.isContinue)),
                  "isForceContinue: %s" % (str(param.isForceContinue)),
                  "mailTo: '%s'" % (",".join(param.mailTo))]
        for i in range(len(parammsg)):
            parammsg[i] = "[RunParameters] " + parammsg[i]
        msg += parammsg
        self._infoLog(msg)



    def _getTaskErrorsSummaryMsg(self, isForceTaskHarvest=False) :
        # isForceHarvest means we try to force an update of the shared
        # taskError information in case this thread is ahead of the
        # task manager.
        if isForceTaskHarvest :
            if (self._tman is not None) and (self._tman.isAlive()) :
                self._tman.harvestTasks()

        if not self._cdata().isTaskError() : return []
        # this case has already been emailed in the TaskManager @ first error occurrence:
        msg = ["Worklow terminated due to the following task errors:"]
        for task in self._cdata().taskErrors :
            msg.extend(task.getTaskErrorMsg())
        return msg


    def _evalWorkflow(self, masterRunStatus) :

        isError = False
        if self._cdata().isTaskError() :
            msg = self._getTaskErrorsSummaryMsg()
            self._errorLog(msg)
            isError = True

        if masterRunStatus.errorCode != 0 :
            eMsg = lister(masterRunStatus.errorMessage)
            if (len(eMsg) > 1) or (len(eMsg) == 1 and eMsg[0] != "") :
                msg = ["Failed to complete master workflow, error code: %s" % (str(masterRunStatus.errorCode))]
                msg.append("errorMessage:")
                msg.extend(eMsg)
                self._notify(msg,logState=LogState.ERROR)
            isError = True

        if self._cdata().isTaskManagerException :
            # this case has already been emailed in the TaskManager:
            self._errorLog("Workflow terminated due to unhandled exception in TaskManager")
            isError = True

        if (not isError) and (not self._tdag.isRunComplete()) :
            msg = "Workflow terminated with unknown error condition"
            self._notify(msg,logState=LogState.ERROR)
            isError = True

        if isError: return 1

        elapsed = int(time.time() - self.runStartTimeStamp)
        msg = []
        if self._cdata().param.successMsg is not None :
            msg.extend([self._cdata().param.successMsg,""])
        msg.extend(["Workflow successfully completed all tasks",
                    "Elapsed time for full workflow: %s sec" % (elapsed)])
        self._notify(msg,logState=LogState.INFO)
        return 0


    def _requireInWorkflow(self) :
        """
        check that the calling method is being called as part of a pyflow workflow() method only
        """
        if not self._getRunning():
            raise Exception("Method must be a (call stack) descendant of WorkflowRunner workflow() method (via run() method)")


    def _initRunning(self):
        try :
            assert(self._isRunning >= 0)
        except AttributeError :
            self._isRunning = 0

    @lockMethod
    def _setRunning(self, isRunning) :
        self._initRunning()
        if isRunning :
            self._isRunning += 1
        else :
            self._isRunning -= 1

    @lockMethod
    def _getRunning(self) :
        self._initRunning()
        return (self._isRunning > 0)



if __name__ == "__main__" :
    help(WorkflowRunner)

