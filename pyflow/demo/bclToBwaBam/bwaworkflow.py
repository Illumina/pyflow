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
bwaworkflow -- a pyflow demonstration module

This is a quick-and-dirty BCL to BWA BAM workflow to demonstrate
how pyflow could be used on a production-scale problem.

__author__ = "Christopher Saunders"
"""


import os.path
import sys

# In production, pyflow can either be installed, or we can distribute
# workflow to external users with pyflow in the same directory/fixed
# relative directory or a configured directory macro-ed in by cmake,
# etc
#
# For now we add the module path by hand:
#
scriptDir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(scriptDir + "/../../src")

from pyflow import WorkflowRunner


#
# utility methods:
#

def ensureDir(d):
    """
    make directory if it doesn't already exist, raise exception is something else is in the way:
    """
    if os.path.exists(d):
        if not os.path.isdir(d) :
            raise Exception("Can't create directory: %s" % (d))
    else :
        os.makedirs(d)


def skipJoin(sep, a, b) :
    if a == "" : return b
    elif b == "" : return a
    return a + sep + b


def preJoin(a, b) :
    return skipJoin('_', a, b)


#
# All of these "flow" functions take a set of task dependencies as
# input and report a set of tasks on output, and thus are designed to
# be plugged together to create workflows which are initiated in
# the WorkflowRunner.workflow() method.
#
# Note that this style is not a design constraint of pyflow, it was
# just one natural way to write the bwa workflow, and demonstrates an
# extensible model wherein various flow functions could be stored in
# external modules and combined as required.
#
# Note that these flow functions are written to lookup dependencies
# from the WorkflowRunner class, so they are really class
# methods. Although they could also lookup data from the derived BWA
# class, they don't -- this allows them to be reused by other
# WorkflowRunner classes.
#



def casava18BclToFastqFlow(self, taskPrefix="", dependencies=set()) :
    """
    CASAVA 1.8 bcl to fastq conversion

    This assumes the bclBasecallsDir is generated in a CASAVA 1.8
    compatible format, and uses CASAVA 1.8 to convert to fastq

    This demonstrates pyflow's makefile handling option, where
    you specify a makefile directory instead of a regular command, and
    pyflow runs make/qmake according to the run mode.

    params:
    casavaDir
    bclBasecallsDir
    flowcellFastqDir
    bclTilePattern
    bclToFastqMaxCores
    """

    # configure bcl2fastq makefile:
    configBclToFastqCmd = "perl %s/bin/configureBclToFastq.pl" % (self.params.casavaDir)
    configBclToFastqCmd += " --input-dir=%s" % self.params.bclBasecallsDir
    configBclToFastqCmd += " --output-dir=%s" % self.params.flowcellFastqDir
    configBclToFastqCmd += " --force"  # always a good idea for CASAVA
    configBclToFastqCmd += " --ignore-missing-bcl"
    configBclToFastqCmd += " --ignore-missing-stats"
    configBclToFastqCmd += " --ignore-missing-control"
    if self.params.bclTilePattern != None :
        configBclToFastqCmd += " --tiles=%s" % (self.params.bclTilePattern)

    # run configuration:
    configLabel = self.addTask(preJoin(taskPrefix, "configBclToFastq"), configBclToFastqCmd, isForceLocal=True, dependencies=dependencies)

    # for the bcl to fastq step, we use another workflow manager, so
    # we just run it as one huge task and handle the mode ourselves:
    nCores = self.getNCores()
    mode = self.getRunMode()

    maxCores = self.params.bclToFastqMaxCores
    if (nCores == "unlimited") or (nCores > maxCores) :
        nCores = maxCores

    # run the fastq conversion:
    bclToFastqLabel = self.addTask(preJoin(taskPrefix, "bclToFastq"),
                                 self.params.flowcellFastqDir,
                                 nCores=nCores,
                                 dependencies=configLabel,
                                 isCommandMakePath=True)

    return set([bclToFastqLabel])



class FastqPairToBwaBamFlow(WorkflowRunner) :
    """
    Given a read1 and read2 pair of fastq files, create an aligned and
    sorted bamFile.  Don't delete input fastaq files.
    """

    def __init__(self, params, suggestedAlignThreadCount=2) :
        """
        suggestedAlignThreadCount -- Number of threads to use in bwa aln
                                     step. The workflow will lower this if
                                     it exceeds the total number of cores
                                     available in the run, or if it
                                     exceeds alnMaxCores

        params:
        fastq1File
        fastq2File
        bamFile
        alnMaxCores
        bwaBin
        genomeFasta
        samtoolsBin
        samtoolsSortMemPerCore
        isKeepFastq
        """
        self.params = params
        self.suggestedAlignThreadCount = suggestedAlignThreadCount


    def workflow(self) :
        bamDir = os.path.dirname(self.params.bamFile)
        ensureDir(bamDir)

        (bamPrefix, bamExt) = os.path.splitext(self.params.bamFile)

        # must end in ".bam" for samtools
        if bamExt != ".bam" :
            raise Exception("bamFile argument must end in '.bam'. bamFile is: %s" % (bamFile))
        if bamPrefix == "" :
            raise Exception("bamFile argument must have a prefix before the '.bam' extension.")

        # assuming many fastq pairs are running, good total throughput given cluster nodes with 2G of ram each
        # should be achieved by given the align processes 2 threads each:

        # grab total cores to make sure we don't exceed it:
        totalCores = self.getNCores()

        #
        # setup aln step:
        #

        # set alnCores
        alnCores = int(self.suggestedAlignThreadCount)
        if (totalCores != "unlimited") and (alnCores > totalCores) :
            alnCores = int(totalCores)
        if (alnCores > self.params.alnMaxCores) :
            alnCores = int(self.params.alnMaxCores)

        bwaBaseCmd = "%s aln -t %i %s" % (self.params.bwaBin, alnCores, self.params.genomeFasta)

        peDependencies = set()

        def getReadLabel(i) : return "Read%iBwaAlign" % (i)
        def getReadSaiFile(i) : return "%s.read%i.sai" % (self.params.bamFile, i)
        def getReadFastqFile(i) : return (self.params.fastq1File, self.params.fastq2File)[i - 1]

        for read in (1, 2) :
            readAlnCmd = "%s %s >| %s" % (bwaBaseCmd, getReadFastqFile(read), getReadSaiFile(read))
            peDependencies.add(self.addTask(getReadLabel(read), readAlnCmd, nCores=alnCores))

        #
        # setup sampe step:
        #

        # with all the pipes, the sampe step is probably a 2 core? this lets sort use more mem too:
        peCores = 2
        if (totalCores != "unlimited") and (peCores > totalCores) :
            peCores = int(totalCores)

        peCmd = "%s sampe %s %s %s %s %s" % (self.params.bwaBin, self.params.genomeFasta,
                                            getReadSaiFile(1), getReadSaiFile(2),
                                            getReadFastqFile(1), getReadFastqFile(2))

        peCmd += " | %s view -uS -" % (self.params.samtoolsBin)

        # For a real pipeline, we'd probably prefer Picard sort, but I don't want to add another
        # dependency to the trial workflow:
        #
        peCmd += " | %s sort -m %i - %s" % (self.params.samtoolsBin,
                                          self.params.samtoolsSortMemPerCore,  # *peCores, need to leave memory for bwa...
                                          bamPrefix)

        peTaskLabel = self.addTask("BwaSamPESort", peCmd, nCores=peCores, dependencies=peDependencies)

        # delete sai files:
        rmCmd = "rm -f"
        for read in (1, 2) :
            rmCmd += " %s" % (getReadSaiFile(read))
        self.addTask("RmSai", rmCmd, dependencies=peTaskLabel, isForceLocal=True)


        # optionally delete input fastqs:
        if not self.params.isKeepFastq :
            fastqRmCmd = "rm -f"
            for read in (1, 2) :
                fastqRmCmd += " %s" % (getReadFastqFile(read))
            self.addTask("RmFastq", fastqRmCmd, dependencies=peTaskLabel, isForceLocal=True)




class FileDigger(object) :
    """
    Digs into a well-defined directory structure with prefixed
    folder names to extract all files associated with
    combinations of directory names.

    This is written primarily to go through the CASAVA 1.8 output
    structure.

    #casava 1.8 fastq example:
    fqDigger=FileDigger('.fastq.gz',['Project_','Sample_'])
    """

    def __init__(self, targetExtension, prefixList) :
        self.targetExtension = targetExtension
        self.prefixList = prefixList

    def getNextFile(self, dir, depth=0, ans=tuple()) :
        """
        generator of a tuple: (flowcell,project,sample,bamfile)
        given a multi-flowcell directory
        """
        if depth < len(self.prefixList) :
            for d in os.listdir(dir) :
                nextDir = os.path.join(dir, d)
                if not os.path.isdir(nextDir) : continue
                if not d.startswith(self.prefixList[depth]) : continue
                value = d[len(self.prefixList[depth]):]
                for val in self.getNextFile(nextDir, depth + 1, ans + tuple([value])) :
                    yield val
        else:
            for f in os.listdir(dir) :
                file = os.path.join(dir, f)
                if not os.path.isfile(file) : continue
                if not f.endswith(self.targetExtension) : continue
                yield ans + tuple([file])



def flowcellDirFastqToBwaBamFlow(self, taskPrefix="", dependencies=set()) :
    """
    Takes as input 'flowcellFastqDir' pointing to the CASAVA 1.8 flowcell
    project/sample fastq directory structure. For each project/sample,
    the fastqs are aligned using BWA, sorted and merged into a single
    BAM file. The bam output is placed in a parallel project/sample
    directory structure below 'flowcellBamDir'

    params:
    samtoolsBin
    flowcellFastqDir
    flowcellBamDir

    calls:
    FastqPairToBwaBamFlow
        supplies:
        bamFile
        fastq1File
        fastq2File
    """

    #
    # 1. separate fastqs into matching pairs:
    #
    fqs = {}
    fqDigger = FileDigger(".fastq.gz", ["Project_", "Sample_"])
    for (project, sample, fqPath) in fqDigger.getNextFile(self.params.flowcellFastqDir) :
        if (self.params.sampleNameList != None) and \
           (len(self.params.sampleNameList) != 0) and \
           (sample not in self.params.sampleNameList) : continue

        fqFile = os.path.basename(fqPath)
        w = (fqFile.split(".")[0]).split("_")
        if len(w) != 5 :
            raise Exception("Unexpected fastq filename format: '%s'" % (fqPath))

        (sample2, index, lane, read, num) = w
        if sample != sample2 :
            raise Exception("Fastq name sample disagrees with directory sample: '%s;" % (fqPath))

        key = (project, sample, index, lane, num)
        if key not in fqs : fqs[key] = [None, None]

        readNo = int(read[1])
        if fqs[key][readNo - 1] != None :
            raise Exceptoin("Unresolvable repeated fastq file pattern in sample: '%s'" % (fqPath))
        fqs[key][readNo - 1] = fqPath


    ensureDir(self.params.flowcellBamDir)

    #
    # 2. run all fastq pairs through BWA:
    #
    nextWait = set()

    for key in fqs.keys() :
        (project, sample, index, lane, num) = key
        sampleBamDir = os.path.join(self.params.flowcellBamDir, "Project_" + project, "Sample_" + sample)
        ensureDir(sampleBamDir)
        keytag = "_".join(key)
        self.params.bamFile = os.path.join(sampleBamDir, keytag + ".bam")
        self.params.fastq1File = fqs[key][0]
        self.params.fastq2File = fqs[key][1]
        nextWait.add(self.addWorkflowTask(preJoin(taskPrefix, keytag), FastqPairToBwaBamFlow(self.params), dependencies=dependencies))

    return nextWait



class FlowcellDirFastqToBwaBamFlow(WorkflowRunner) :
    """
    Takes as input 'flowcellFastqDir' pointing to the CASAVA 1.8 flowcell
    project/sample fastq directory structure. For each project/sample,
    the fastqs are aligned using BWA, sorted and merged into a single
    BAM file. The bam output is placed in a parallel project/sample
    directory structure below 'flowcellBamDir'

    params:
    flowcellFastqDir
    flowcellBamDir
    """

    def __init__(self, params) :
        self.params = params

    def workflow(self) :
        flowcellDirFastqToBwaBamFlow(self)





# use a really boring flowcell label everywhere right now:
def getFlowcellLabel(self, i) :
    return "Flowcell_FC%i" % (i)




def casava18BclToBamListFlow(self, taskPrefix="", dependencies=set()) :
    """
    Runs bcl conversion and alignment on multiple flowcells for a subset of samples.
    Writes BAM files to parallel fastq Project/Sample directory structure. Does not
    merge individual BAMs. Deletes fastqs on alignment when option is set to do so.

    params:
    allFlowcellDir
    bclBasecallsDirList
    bclTilePatternList

    calls:
    casava18BclToFastqFlow
        supplies:
        bclBasecallsDir
        flowcellFastqDir
    FlowcellDirFastqToBwaBamFlow
        supplies:
        flowcellFastqDir
        flowcellBamDir

    """

    ensureDir(self.params.allFlowcellDir)

    # first bcl->fastq->bwa bam for requested samples in all flowcells:
    nextWait = set()
    for i, self.params.bclBasecallsDir in enumerate(self.params.bclBasecallsDirList) :
        flowcellLabel = getFlowcellLabel(self, i)
        flowcellDir = os.path.join(self.params.allFlowcellDir, flowcellLabel)

        ensureDir(flowcellDir)

        self.params.flowcellFastqDir = os.path.join(flowcellDir, "fastq")
        self.params.flowcellBamDir = os.path.join(flowcellDir, "bam")
        if self.params.bclTilePatternList == None :
            self.params.bclTilePattern = None
        else :
            self.params.bclTilePattern = self.params.bclTilePatternList[i]

        fastqFinal = casava18BclToFastqFlow(self, taskPrefix=flowcellLabel)

        label = preJoin(taskPrefix, "_".join((flowcellLabel, "FastqToBwaBam")))
        nextWait.add(self.addWorkflowTask(label, FlowcellDirFastqToBwaBamFlow(self.params), dependencies=fastqFinal))

    return nextWait




def mergeBamListFlow(self, taskPrefix="", dependencies=set()) :
    """
    Take a list of sorted bam files from the same sample, merge them together,
    and delete input bams, final output to mergeBamName

    params:
    mergeBamList
    mergeBamName
    samtoolsBin
    """

    for bamFile in self.params.mergeBamList :
        if not os.path.isfile(bamFile) :
            raise Exception("Can't find bam file: '%s'" % (bamFile))

    mergeTasks = set()
    mergeLabel = preJoin(taskPrefix, "merge")
    if len(self.params.mergeBamList) > 1 :
        mergeCmd = "%s merge -f %s %s" % (self.params.samtoolsBin, self.params.mergeBamName, " ".join(self.params.mergeBamList))
        mergeTasks.add(self.addTask(mergeLabel, mergeCmd, dependencies=dependencies, isTaskStable=False))

        rmCmd = "rm -f"
        for bamFile in self.params.mergeBamList :
            rmCmd += " %s" % (bamFile)

        self.addTask(preJoin(taskPrefix, "rmBam"), rmCmd, dependencies=mergeLabel, isForceLocal=True)
    elif len(self.params.mergeBamList) == 1 :
        mvCmd = "mv %s %s" % (self.params.mergeBamList[0], self.params.mergeBamName)
        # *must* have same taskLabel as merge command for continuation
        # to work correctly because of the potential for partial
        # deletion of the input bam files:
        mergeTasks.add(self.addTask(mergeLabel, mvCmd, dependencies=dependencies, isForceLocal=True, isTaskStable=False))

    return mergeTasks



def flowcellBamListMergeFlow(self, taskPrefix="", dependencies=set()) :
    """
    given a root flowcell directory and list of samples, merge sample
    bams across flowcells and dedup.

    ?? Will we be in a situation where sample has more than one library
    -- this affects the debup order & logic ??

    params:
    allFlowcellDir
    mergedDir
    sampleNameList
    picardDir

    calls:
    mergeBamListFlow
        supplies:
        mergeBamList
        mergeBamName
    """

    #
    # 1) get a list of bams associated with each project/sample combination:
    #

    # TODO: what if there's an NFS delay updating all the bams while
    # we're reading them out here? make this process more robust -- we
    # should know how many BAM's we're expecting, in a way that's
    # robust to interuption/restart
    #
    bams = {}
    bamDigger = FileDigger(".bam", ["Flowcell_", "bam", "Project_", "Sample_"])
    for (flowcell, nothing, project, sample, bamFile) in bamDigger.getNextFile(self.params.allFlowcellDir) :
        if (self.params.sampleNameList != None) and \
           (len(self.params.sampleNameList) != 0) and \
           (sample not in self.params.sampleNameList) : continue
        key = (project, sample)
        if key not in bams : bams[key] = []
        bams[key].append(bamFile)

    mergedBamExt = ".merged.bam"
    markDupBamExt = ".markdup.bam"

    #
    # 2) merge and delete smaller bams:
    #
    mergedBams = {}

    mergedBamDir = os.path.join(self.params.mergedDir, "bam")
    sampleTasks = {}
    if len(bams) :  # skip this section if smaller bams have already been deleted
        ensureDir(mergedBamDir)

        for key in bams.keys() :
            (project, sample) = key
            mergedSampleDir = os.path.join(mergedBamDir, "Project_" + project, "Sample_" + sample)
            ensureDir(mergedSampleDir)
            self.params.mergeBamList = bams[key]
            self.params.mergeBamName = os.path.join(mergedSampleDir, sample + mergedBamExt)
            mergedBams[key] = self.params.mergeBamName
            outTaskPrefix = preJoin(taskPrefix, "_".join(key))
            sampleTasks[key] = mergeBamListFlow(self, outTaskPrefix, dependencies)

    if not os.path.isdir(mergedBamDir) : return


    #
    # 3) mark dup:
    #

    # mergedBams contains all bams from the current run, we also add any from a
    # previous interupted run:
    mergedBamDigger = FileDigger(mergedBamExt, ["Project_", "Sample_"])
    for (project, sample, bamFile) in mergedBamDigger.getNextFile(mergedBamDir) :
        key = (project, sample)
        if key in mergedBams :
            assert (mergedBams[key] == bamFile)
        else :
            mergedBams[key] = bamFile

    nextWait = set()
    totalCores = self.getNCores()

    for sampleKey in mergedBams.keys() :
        markDupDep = set()
        if sampleKey in sampleTasks : markDupDep = sampleTasks[sampleKey]

        fullName = "_".join(sampleKey)

        markDupBamFile = mergedBams[sampleKey][:-(len(mergedBamExt))] + markDupBamExt
        markDupMetricsFile = markDupBamFile[:-(len(".bam"))] + ".metrics.txt"
        markDupTmpDir = markDupBamFile + ".tmpdir"

        # for now, solve the memory problem with lots of threads:
        nCores = 4
        if (totalCores != "unlimited") and (totalCores < nCores) :
            nCores = totalCores
        gigs = 2 * nCores
        javaOpts = "-Xmx%ig" % (gigs)
        markDupFiles = "INPUT=%s OUTPUT=%s METRICS_FILE=%s" % (mergedBams[sampleKey], markDupBamFile, markDupMetricsFile)
        markDupOpts = "REMOVE_DUPLICATES=false ASSUME_SORTED=true VALIDATION_STRINGENCY=SILENT CREATE_INDEX=true TMP_DIR=%s" % (markDupTmpDir)
        markDupJar = os.path.join(self.params.picardDir, "MarkDuplicates.jar")
        markDupCmd = "java %s -jar %s %s %s" % (javaOpts, markDupJar, markDupFiles, markDupOpts)
        markDupTask = self.addTask(preJoin(taskPrefix, fullName + "_dupmark"), markDupCmd, dependencies=markDupDep)

        # link index filename to something samtools can understand:
        #
        markDupPicardBaiFile = markDupBamFile[:-(len(".bam"))] + ".bai"
        markDupSamtoolsBaiFile = markDupBamFile + ".bai"
        indexLinkCmd = "ln %s %s" % (markDupPicardBaiFile, markDupSamtoolsBaiFile)
        indexLinkTask = self.addTask(preJoin(taskPrefix, fullName + "_indexLink"), indexLinkCmd, dependencies=markDupTask, isForceLocal=True)

        nextWait.add(indexLinkTask)

        # delete TmpDir:
        #
        rmMarkDupTmpCmd = "rm -rf %s" % (markDupTmpDir)
        self.addTask(preJoin(taskPrefix, fullName + "_rmMarkDupTmp"), rmMarkDupTmpCmd, dependencies=markDupTask, isForceLocal=True)

        # now remove the original file:
        #
        rmCmd = "rm -f %s" % (mergedBams[sampleKey])
        self.addTask(preJoin(taskPrefix, fullName + "_rmMerge"), rmCmd, dependencies=markDupTask, isForceLocal=True)

    return nextWait




class FlowcellBamListMergeFlow(WorkflowRunner) :

    def __init__(self, params) :
        self.params = params

    def workflow(self) :
        flowcellBamListMergeFlow(self)



class BWAWorkflow(WorkflowRunner) :
    """
    pyflow BCL to BAM BWA workflow
    """

    def __init__(self, params) :
        self.params = params

        # make sure working directory is setup:
        self.params.outputDir = os.path.abspath(self.params.outputDir)
        ensureDir(self.params.outputDir)

        self.params.allFlowcellDir = os.path.join(self.params.outputDir, "flowcell_results")
        self.params.mergedDir = os.path.join(self.params.outputDir, "merged_results")

        # Verify/manipulate various input options:
        #
        # this is mostly repeated in the conflig script now... get this minimized with auto verification:
        #
        self.params.bclBasecallsDirList = map(os.path.abspath, self.params.bclBasecallsDirList)
        for dir in self.params.bclBasecallsDirList :
            if not os.path.isdir(dir) :
                raise Exception("Input BCL basecalls directory not found: '%s'" % (dir))

        self.params.samtoolsSortMemPerCore = int(self.params.samtoolsSortMemPerCore)
        minSortMem = 1000000
        if self.params.samtoolsSortMemPerCore < minSortMem :
            raise Exception("samtoolsSortMemPerCore must be an integer greater than minSortMem")

        if self.params.genomeFasta == None:
            raise Exception("No bwa genome file defined.")
        else:
            if not os.path.isfile(self.params.genomeFasta) :
                raise Exception("Can't find bwa genome file '%s'" % (self.params.genomeFasta))


    def workflow(self) :

        alignTasks = casava18BclToBamListFlow(self)
        mergeTask = self.addWorkflowTask("mergeBams", FlowcellBamListMergeFlow(self.params), dependencies=alignTasks)



