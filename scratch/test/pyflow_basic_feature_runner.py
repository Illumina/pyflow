#!/usr/bin/env python
"""
this is a script which runs a lot of features...
it does not provide test coverage as to whether everything
ran correctly... it will only pick up a basic crash or hang.
"""

import os.path
import sys

# bad example of how to add the path:
scriptDir=os.path.abspath(os.path.dirname(__file__))


# setup PYTHONPATH instead...

#sys.path.append(scriptDir+"/../pyflow/src")

from pyflow import WorkflowRunner


testJobDir=os.path.join(scriptDir,"testtasks")

sleepjob=os.path.join(testJobDir,"sleeper.bash")
yelljob=os.path.join(testJobDir,"yeller.bash")
runjob=os.path.join(testJobDir,"runner.bash")

class SubSubWorkflow(WorkflowRunner) :

    def workflow(self) :
        self.addTask("easy_task1",yelljob+" 1")
        self.addTask("easy_task2",runjob+" 2",nCores=3,dependencies=["easy_task1"])
        self.waitForTasks("easy_task2")
        self.addTask("easy_task3",runjob+" 2",nCores=3,dependencies=["easy_task2"])
        # intentional fail:
        #self.addTask("easy_task3b",runjob,dependencies=["easy_task2"])


class SubWorkflow(WorkflowRunner) :

    def workflow(self) :
        self.addTask("easy_task1",yelljob+" 1")
        self.addTask("easy_task2",runjob+" 2",nCores=3,dependencies=["easy_task1"])
        self.addTask("easy_task3",runjob+" 2",nCores=3,dependencies=["easy_task2"])
        wflow=SubSubWorkflow()
        self.addWorkflowTask("subsubwf_task1",wflow,dependencies="easy_task1")



class TestWorkflow(WorkflowRunner) :

    def workflow(self) :

	job=sleepjob+" 1"

        self.addTask("easy_task1",yelljob+" 1")
        waitTask=self.addTask("easy_task3",runjob+" 10",nCores=2,memMb=1024,isForceLocal=True)
        self.flowLog("My message")

        swflow=SubWorkflow()

        self.addWorkflowTask("subwf_task1",swflow,dependencies=waitTask)
        self.addWorkflowTask("subwf_task2",swflow,dependencies=waitTask)

        self.addTask("easy_task4",runjob+" 2",nCores=3,dependencies=["subwf_task1","subwf_task2"])
        self.addTask("easy_task5",job,nCores=1)

        # and stop here
        self.waitForTasks()

        self.flowLog("ITC1: "+str(self.isTaskComplete("easy_task1")))
        self.flowLog("ITC6: "+str(self.isTaskComplete("easy_task6")))

        self.addTask("easy_task6",job)
        #self.addTask("easy_task2",sleepjob)
        self.addTask("checkpoint_task",dependencies=["easy_task1","easy_task6","easy_task4"])
        self.addTask("dep_task",sleepjob+" 4",dependencies=["checkpoint_task"])



def getRunOptions() :

    from optparse import OptionParser

    defaults = { "mode" : "local" }

    parser = OptionParser()
    parser.set_defaults(**defaults)

    parser.add_option("-m", "--mode", type="string", dest="mode",
                      help="Select run mode {local,sge} (default: %default)")

    (options, args) = parser.parse_args()

    if len(args) :
        parser.print_help()
        sys.exit(2)

    if options.mode not in ["sge","local"] :
        parser.print_help()
        sys.exit(2)

    return options



def main() :
    options = getRunOptions()
    wflow = TestWorkflow()
    retval=wflow.run(options.mode,nCores=8,memMb=8*1024,isContinue=False)
    sys.exit(retval)



if __name__ == "__main__" :
    main()
