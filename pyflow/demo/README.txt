
This directory contains small demonstration workflows for various
pyflow features. If you are new to pyflow, a recommended order to
become familiar with its features is:

1. helloWorld
This demonstrates a minimum single-task pyflow workflow.

2. simpleDemo
This workflow demonstrates a number of commonly used pyflow features
by setting up a number of tasks and showing different ways to specify
task resource requirements and dependencies.

3. subWorkflow 
This workflow demonstrates the more advanced workflow recursion feature.

4. runOptionsDemo
This workflow demostrates one possible way the pyflow API runtime
options could be translated to user command-line arguments if building
a command-line utility.

5. bclToBwaBam
This workflow demonstrates a much larger 'real-world' script which
performs bcl to fasta conversion from mulitple flowcells, alignment
with BWA and translation of the BWA output to a single sorted and
indexed BAM file. It has numerous dependencies required to actually
run -- it's primary purpose here is to provide an example of how a
larger scale pyflow workflow might look.


Most of the remaining workflows demonstrate/test the use of specific
pyflow features.

