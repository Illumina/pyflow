#!/usr/bin/env python

import unittest
import os
import sys

scriptDir=os.path.abspath(os.path.dirname(__file__))

def pyflow_lib_dir() :
    return os.path.abspath(os.path.join(scriptDir,os.pardir,os.pardir,"pyflow","src"))

try :
    # if pyflow is in PYTHONPATH already then use the specified copy:
    from pyflow import isWindows,WorkflowRunner
except :
    # otherwise use the relative path within this repo:
    sys.path.append(pyflow_lib_dir())
    from pyflow import isWindows,WorkflowRunner


def getRmCmd() :
    if isWindows():
        return ["del","/f"]
    else:
        return ["rm","-f"]


def getSleepCmd() :
    if isWindows():
        return ["timeout"]
    else:
        return ["sleep"]


def getCatCmd() :
    if isWindows():
        return ["type"]
    else:
        return ["cat"]


def getCmdString(cmdList) :
    return " ".join(cmdList)



class NullWorkflow(WorkflowRunner) :
    pass



class TestWorkflowRunner(unittest.TestCase) :

    def __init__(self, *args, **kw) :
        unittest.TestCase.__init__(self, *args, **kw)
        self.testPath="testDataRoot"

    def setUp(self) :
        self.clearTestPath()

    def tearDown(self) :
        self.clearTestPath()

    def clearTestPath(self) :
        import shutil
        if os.path.isdir(self.testPath) :
            shutil.rmtree(self.testPath)


    def test_createDataDir(self) :
        w=NullWorkflow()
        w.run("local",self.testPath,isQuiet=True)
        self.assertTrue(os.path.isdir(self.testPath))


    def test_badMode(self) :
        w=NullWorkflow()
        try:
            w.run("foomode",self.testPath,isQuiet=True)
            self.fail("Didn't raise Exception")
        except KeyError:
            self.assertTrue(sys.exc_info()[1].args[0].find("foomode") != -1)


    def test_errorLogPositive(self) :
        """
        Test that errors are written to separate log when requested
        """
        os.mkdir(self.testPath)
        logFile=os.path.join(self.testPath,"error.log")
        w=NullWorkflow()
        try:
            w.run("foomode",self.testPath,errorLogFile=logFile,isQuiet=True)
            self.fail("Didn't raise Exception")
        except KeyError:
            self.assertTrue(sys.exc_info()[1].args[0].find("foomode") != -1)
        self.assertTrue((os.path.getsize(logFile) > 0))


    def test_errorLogNegative(self) :
        """
        Test that no errors are written to separate error log when none occur
        """
        os.mkdir(self.testPath)
        logFile=os.path.join(self.testPath,"error.log")
        w=NullWorkflow()
        w.run("local",self.testPath,errorLogFile=logFile,isQuiet=True)
        self.assertTrue((os.path.getsize(logFile) == 0))


    def test_dataDirCollision(self) :
        """
        Test that when two pyflow jobs are launched with the same dataDir, the second will fail.
        """
        import threading,time

        class StallWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("sleeper",getSleepCmd()+["5"])

        class runner(threading.Thread) :
            def __init__(self2) :
                threading.Thread.__init__(self2)
                self2.retval1=1

            def run(self2) :
                w=StallWorkflow()
                self2.retval1=w.run("local",self.testPath,isQuiet=True)

        w2=StallWorkflow()

        r1=runner()
        r1.start()
        time.sleep(1)
        retval2=w2.run("local",self.testPath,isQuiet=True)
        self.assertTrue(retval2==1)
        r1.join()
        self.assertTrue(r1.retval1==0)


    def test_forceContinue(self) :
        class TestWorkflow(WorkflowRunner) :
            color="red"

            def setColor(self2,color) :
                self2.color=color

            def workflow(self2) :
                self2.addTask("A","echo "+self2.color)

        w=TestWorkflow()
        retval=w.run("local",self.testPath,isQuiet=True)
        self.assertEqual(retval, 0)
        retval=w.run("local",self.testPath,isContinue=True,isQuiet=True)
        self.assertEqual(retval, 0)
        w.setColor("green")
        retval=w.run("local",self.testPath,isContinue=True,isQuiet=True)
        self.assertEqual(retval, 1)
        retval=w.run("local",self.testPath,isContinue=True,isForceContinue=True,isQuiet=True)
        self.assertEqual(retval, 0)


    def test_badContinue(self) :
        w=NullWorkflow()
        try:
            w.run("local",self.testPath,isContinue=True,isQuiet=True)
            self.fail("Didn't raise Exception")
        except Exception:
            self.assertTrue(sys.exc_info()[1].args[0].find("Cannot continue run") != -1)


    def test_goodContinue(self) :
        w=NullWorkflow()
        retval1=w.run("local",self.testPath,isQuiet=True)
        retval2=w.run("local",self.testPath,isContinue=True,isQuiet=True)
        self.assertTrue((retval1==0) and (retval2==0))


    def test_autoContinue(self) :
        w=NullWorkflow()
        retval1=w.run("local",self.testPath,isContinue="Auto",isQuiet=True)
        retval2=w.run("local",self.testPath,isContinue="Auto",isQuiet=True)
        self.assertTrue((retval1==0) and (retval2==0))


    def test_simpleDependency(self) :
        "make sure B waits for A"
        class TestWorkflow(WorkflowRunner) :
            def workflow(self2) :
                filePath=os.path.join(self.testPath,"tmp.txt")
                self2.addTask("A","echo foo > " +filePath)
                self2.addTask("B",getCmdString(getCatCmd()) + " " + filePath + " && " + getCmdString(getRmCmd())+ " " + filePath,dependencies="A")

        w=TestWorkflow()
        self.assertEqual(w.run("local",self.testPath,isQuiet=True), 0)


    def test_waitDependency(self) :
        "make sure waitForTasks waits for A on the workflow thread"
        class TestWorkflow(WorkflowRunner) :
            def workflow(self2) :
                filePath=os.path.join(self.testPath,"tmp.txt")
                if os.path.isfile(filePath) : os.remove(filePath)
                self2.addTask("A",getCmdString(getSleepCmd()) + " 5 && echo foo > %s" % (filePath))
                self2.waitForTasks("A")
                assert(os.path.isfile(filePath))
                self2.addTask("B",getCmdString(getCatCmd()) + " " + filePath +" && " + getCmdString(getRmCmd())+ " " + filePath)

        w=TestWorkflow()
        self.assertTrue(0==w.run("local", self.testPath, isQuiet=True))


    def test_flowLog(self) :
        "make sure flowLog doesn't throw -- but this does not check if the log is updated"
        class TestWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.flowLog("My Message")

        w=TestWorkflow()
        self.assertTrue(0==w.run("local",self.testPath,isQuiet=True))


    def test_deadSibling(self) :
        """
        Tests that when a task error occurs in one sub-workflow, its
        sibling workflows exit correctly (instead of hanging forever).
        This test is an early library error case.
        """
        class SubWorkflow1(WorkflowRunner) :
            "This workflow should fail."
            def workflow(self2) :
                self2.addTask("A",getSleepCmd()+["5"])
                self2.addTask("B","boogyman!",dependencies="A")
                
        class SubWorkflow2(WorkflowRunner) :
            "This workflow should not fail."
            def workflow(self2) :
                self2.addTask("A",getSleepCmd()+["5"])
                self2.addTask("B",getSleepCmd()+["5"],dependencies="A")

        class MasterWorkflow(WorkflowRunner) :
            def workflow(self2) :
                wflow1=SubWorkflow1()
                wflow2=SubWorkflow2()
                self2.addWorkflowTask("wf1",wflow1)
                self2.addWorkflowTask("wf2",wflow2)

        w=MasterWorkflow()
        self.assertTrue(1==w.run("local",self.testPath,nCores=2,isQuiet=True))


    def test_selfDependency1(self) :
        """
        """
        class SelfWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("A",getSleepCmd()+["5"],dependencies="A")
                
        w=SelfWorkflow()
        self.assertEqual(w.run("local",self.testPath,isQuiet=True), 1)


    def test_expGraphScaling(self) :
        """
        This tests that pyflow does not scale poorly with highly connected subgraphs.

        When the error occurs, it locks the primary thread, so we put the test workflow
        on its own thread so that we can time it and issue an error.

        Issue reported by R Kelley and A Halpern
        """

        import threading

        class ScalingWorkflow(WorkflowRunner) :
            def workflow(self2) :
                tasks = set()
                for idx in xrange(60) :
                    sidx = str(idx)
                    tasks.add(self2.addTask("task_" + sidx, "echo " + sidx, dependencies = tasks))
                self2.waitForTasks("task_50")
                tasks.add(self2.addTask("task_1000", "echo 1000", dependencies = tasks))

        class runner(threading.Thread) :
            def __init__(self2) :
                threading.Thread.__init__(self2)
                self2.setDaemon(True)

            def run(self2) :
                w=ScalingWorkflow()
                w.run("local",self.testPath,isQuiet=True)

        r1=runner()
        r1.start()
        r1.join(30)
        self.assertTrue(not r1.isAlive())

    def test_startFromTasks(self) :
        """
        run() option to ignore all tasks before a specified task node
        """
        filePath=os.path.join(self.testPath,"tmp.txt")

        class SelfWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("A","echo foo > "+filePath)
                self2.addTask("B",getSleepCmd()+["1"],dependencies="A")
                self2.addTask("C",getSleepCmd()+["1"],dependencies=("A","B"))
 
        w=SelfWorkflow()
        self.assertEqual(w.run("local",self.testPath,isQuiet=True,startFromTasks="B"), 0)
        self.assertFalse(os.path.exists(filePath))


    def test_startFromTasksSubWflow(self) :
        """
        run() option to ignore all tasks before a specified task node
        """
        filePath=os.path.join(self.testPath,"tmp.txt")

        class SubWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("D","echo foo > "+filePath)

        class SelfWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("A",getSleepCmd()+["1"])
                self2.addWorkflowTask("B",SubWorkflow(),dependencies="A")
                self2.addTask("C",getSleepCmd()+["1"],dependencies=("A","B"))

        w=SelfWorkflow()
        self.assertEqual(w.run("local",self.testPath,isQuiet=True,startFromTasks="B"), 0)
        self.assertTrue(os.path.exists(filePath))


    def test_startFromTasksSubWflow2(self) :
        """
        run() option to ignore all tasks before a specified task node
        """
        filePath=os.path.join(self.testPath,"tmp.txt")

        class SubWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("D","echo foo > "+filePath)

        class SelfWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("A",getSleepCmd()+["1"])
                self2.addWorkflowTask("B",SubWorkflow(),dependencies="A")
                self2.addTask("C",getSleepCmd()+["1"],dependencies=("A","B"))

        w=SelfWorkflow()
        self.assertEqual(w.run("local",self.testPath,isQuiet=True,startFromTasks="C"), 0)
        self.assertFalse(os.path.exists(filePath))


    def test_ignoreTasksAfter(self) :
        """
        run() option to ignore all tasks below a specified task node
        """
        class SelfWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("A",getSleepCmd()+["1"])
                self2.addTask("B",getSleepCmd()+["1"],dependencies="A")
                self2.addTask("C",getSleepCmd()+["1"],dependencies=("A","B"))
 
        w=SelfWorkflow()
        self.assertEqual(w.run("local",self.testPath,isQuiet=True,ignoreTasksAfter="B"), 0)
        self.assertFalse(w.isTaskComplete("C"))

    def test_addTaskOutsideWorkflow(self) :
        """
        test that calling addTask() outside of a workflow() method
        raises an exception
        """

        class SelfWorkflow(WorkflowRunner) :
            def __init__(self2) :
                self2.addTask("A",getSleepCmd()+["1"])

        try :
            w=SelfWorkflow()
            self.fail("Didn't raise Exception")
        except :
            pass

    def test_runModeInSubWorkflow(self) :
        """
        test that calling getRunMode() in a sub-workflow() method
        does not raise an exception (github issue #5)
        """

        class SubWorkflow(WorkflowRunner) :
            def workflow(self2) :
                if self2.getRunMode() == "local" :
                    self2.addTask("D",getSleepCmd()+["1"])

        class SelfWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("A",getSleepCmd()+["1"])
                self2.addWorkflowTask("B",SubWorkflow(),dependencies="A")
                self2.addTask("C",getSleepCmd()+["1"],dependencies=("A","B"))

        try :
            w=SelfWorkflow()
            self.assertEqual(w.run("local",self.testPath,isQuiet=True), 0)
        except :
            self.fail("Should not raise Exception")

    def test_checkpointChain(self) :
        """
        Test that checkout points are handled correctly even
        when multiple checkpoints have a parent-child relationship
        """

        class SelfWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("A")
                self2.addTask("B")
                self2.addTask("C",dependencies=["A","B"])

        try :
            w=SelfWorkflow()
            self.assertEqual(w.run("local",self.testPath,isQuiet=True), 0)
        except :
            self.fail("Should not raise Exception")

    def test_cancelTaskTree(self) :
        """
        Test that tasks can be canceled.
        """

        class SelfWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("A", getSleepCmd()+["3"])
                self2.addTask("B", getSleepCmd()+["30"], dependencies="A")
                self2.addTask("C", getSleepCmd()+["30"], dependencies="B")
                import time
                time.sleep(1)
                self2.cancelTaskTree("B")

        try :
            w=SelfWorkflow()
            self.assertEqual(w.run("local",self.testPath,isQuiet=True), 0)
            self.assertTrue(w.isTaskComplete("A"))
            self.assertFalse(w.isTaskComplete("B"))
            self.assertFalse(w.isTaskComplete("C"))
        except :
            self.fail("Should not raise Exception")

    def test_isTaskDone(self) :
        """
        Test new isTaskDone() method
        """

        class SelfWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("A",getSleepCmd()+["1"])
                self2.addTask("B","boogyman!",dependencies="A")

        try :
            w=SelfWorkflow()
            self.assertEqual(w.run("local", self.testPath, isQuiet=True), 1)
            self.assertEqual(w.isTaskDone("A"), (True, False))
            self.assertEqual(w.isTaskDone("B"), (True, True))
        except :
            self.fail("Should not raise Exception")

    def test_queryMissingTask(self) :
        """
        Test query on an undefined task name
        """

        class SelfWorkflow(WorkflowRunner) :
            def workflow(self2) :
                self2.addTask("A",getSleepCmd()+["1"])

        try :
            w=SelfWorkflow()
            self.assertEqual(w.run("local", self.testPath, isQuiet=True), 0)
            self.assertEqual(w.isTaskComplete("B"), False)
        except :
            self.fail("Should not raise Exception")



if __name__ == '__main__' :
    unittest.main()
