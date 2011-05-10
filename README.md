#jobTree#
Benedict Paten, benedict (a) soe ucsc edu

###Job-tree Introduction:###

Most batch systems (such as LSF, Parasol, etc.) do not allow jobs to spawn
other jobs in a simple way. Job-tree offers up a simple Python based batch
control module. Use job-tree to create dynamically branching jobs on your 
cluster. With job-tree you don't need to know the exact structure of the 
entire set of jobs, you just need to specify a job's child jobs and its 
follow-on job and job-tree takes care of the rest.
... details in <code>docs/README.rtf</code>

###Script-tree Introduction:###

Script-tree provides a simple Python interface to interact with job-tree.
... details in <code>docs/README.rtf</code>
