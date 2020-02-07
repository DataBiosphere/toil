from toil.job import Job
from toil.common import Toil
from toil.version import python
from toil import subprocess
import os
import logging

logger = logging.getLogger(__name__)

'''
This workflow's purpose is to create files and jobs for viewing using stats, 
status, and printDot() in toilDebugTest.py.  It's intended for future use in a 
debugging tutorial containing a broken job.  It is also a minor integration test.
'''

def initialize_jobs(job):
    '''
    Stub function used to start a toil workflow since toil workflows can only
    start with one job (but afterwards can run many in parallel).
    '''
    job.fileStore.logToMaster('''initialize_jobs''')

def writeA(job, mkFile):
    '''Runs a program, and writes a string 'A' into A.txt using mkFile.py.'''
    job.fileStore.logToMaster('''writeA''')

    # temp folder for the run
    tempDir = job.fileStore.getLocalTempDir()

    # import files
    try:
        mkFile_fs = job.fileStore.readGlobalFile(mkFile[0], userPath=os.path.join(tempDir, mkFile[1]))
    except:
        mkFile_fs = os.path.join(tempDir, mkFile[1])

    # make a file (A.txt) and writes a string 'A' into it using 'mkFile.py'
    content = 'A'
    cmd = python + ' ' + mkFile_fs + ' ' + 'A.txt' + ' ' + content
    this_process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    this_process.wait()

    # get the output file and return it as a tuple of location + name
    output_filename = 'A.txt'
    output_file = job.fileStore.writeGlobalFile(output_filename)
    A1 = (output_file, output_filename)
    rvDict = {"A1": A1}
    return rvDict

def writeB(job, mkFile, B_file):
    '''
    Runs a program, extracts a string 'B' from an existing file, B_file.txt, and
    writes it into B.txt using mkFile.py.
    '''
    job.fileStore.logToMaster('''writeB''')

    # temp folder for the run
    tempDir = job.fileStore.getLocalTempDir()

    # import files
    try:
        mkFile_fs = job.fileStore.readGlobalFile(mkFile[0], userPath=os.path.join(tempDir, mkFile[1]))
    except:
        mkFile_fs = os.path.join(tempDir, mkFile[1])
    try:
        B_file_fs = job.fileStore.readGlobalFile(B_file[0], userPath=os.path.join(tempDir, B_file[1]))
    except:
        B_file_fs = os.path.join(tempDir, B_file[1])

    # make a file (B.txt) and write the contents of 'B_file.txt' into it using 'mkFile.py'
    with open(B_file_fs, "r") as f:
        file_contents = ''
        for line in f:
            file_contents = file_contents + line

    cmd = python + ' ' + mkFile_fs + ' ' + 'B.txt' + ' ' + file_contents
    this_process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    this_process.wait()

    # get the output file and return it as a tuple of location + name
    output_filename = 'B.txt'
    output_file = job.fileStore.writeGlobalFile(output_filename)
    B1 = (output_file, output_filename)
    rvDict = {"B1": B1}
    return rvDict

def writeC(job):
    '''Creates/writes a file, C.txt, containing the string 'C'.'''
    job.fileStore.logToMaster('''writeC''')

    # temp folder for the run
    tempDir = job.fileStore.getLocalTempDir()

    # get the output file and return it as a tuple of location + name
    output_filename = os.path.join(tempDir, 'C.txt')
    with open(output_filename, 'w') as f:
        f.write('C')
    output_file = job.fileStore.writeGlobalFile(output_filename)
    C1 = (output_file, output_filename)
    rvDict = {"C1": C1}
    return rvDict

def writeABC(job, A_dict, B_dict, C_dict, filepath):
    '''Takes 3 files (specified as dictionaries) and writes their contents to ABC.txt.'''
    job.fileStore.logToMaster('''writeABC''')

    # temp folder for the run
    tempDir = job.fileStore.getLocalTempDir()

    # import files
    try:
        A_fs = job.fileStore.readGlobalFile(A_dict['A1'][0], userPath=os.path.join(tempDir, A_dict['A1'][1]))
    except:
        A_fs = os.path.join(tempDir, A_dict['A1'][1])
    try:
        B_fs = job.fileStore.readGlobalFile(B_dict['B1'][0], userPath=os.path.join(tempDir, B_dict['B1'][1]))
    except:
        B_fs = os.path.join(tempDir, B_dict['B1'][1])
    try:
        C_fs = job.fileStore.readGlobalFile(C_dict['C1'][0], userPath=os.path.join(tempDir, C_dict['C1'][1]))
    except:
        C_fs = os.path.join(tempDir, C_dict['C1'][1])

    file_contents = ''
    with open(A_fs, "r") as f:
        for line in f:
            file_contents = file_contents + line

    with open(B_fs, "r") as f:
        for line in f:
            file_contents = file_contents + line

    with open(C_fs, "r") as f:
        for line in f:
            file_contents = file_contents + line

    with open(os.path.join(tempDir, 'ABC.txt'), 'w') as f:
        f.write(file_contents)

    # get the output file and return it as a tuple of location + name
    output_filename = os.path.join(tempDir, 'ABC.txt')
    output_file = job.fileStore.writeGlobalFile(output_filename)
    job.fileStore.exportFile(output_file, "file://" + filepath)


def finalize_jobs(job, num):
    '''Does nothing but should be recorded in stats, status, and printDot().'''
    job.fileStore.logToMaster('''finalize_jobs''')

def broken_job(job, num):
    '''A job that will always fail.  To be used for a tutorial.'''
    job.fileStore.logToMaster('''broken_job''')
    file = toil.importFile(None)

if __name__=="__main__":
    options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
    # options.clean = "always"
    options.stats = True
    options.logLevel = "INFO"
    with Toil(options) as toil:

        B_file0 = toil.importFile("file://" + os.path.abspath("src/toil/test/utils/ABCWorkflowDebug/B_file.txt"))
        B_file0_preserveThisFilename = "B_file.txt"
        B_file = (B_file0, B_file0_preserveThisFilename)

        file_maker0 = toil.importFile("file://" + os.path.abspath("src/toil/test/utils/ABCWorkflowDebug/mkFile.py"))
        file_maker0_preserveThisFilename = "mkFile.py"
        file_maker = (file_maker0, file_maker0_preserveThisFilename)

        filepath = os.path.abspath("src/toil/test/utils/ABCWorkflowDebug/ABC.txt")

        job0 = Job.wrapJobFn(initialize_jobs)
        job1 = Job.wrapJobFn(writeA, file_maker)
        job2 = Job.wrapJobFn(writeB, file_maker, B_file)
        job3 = Job.wrapJobFn(writeC)
        job4 = Job.wrapJobFn(writeABC, job1.rv(), job2.rv(), job3.rv(), filepath)
        job5 = Job.wrapJobFn(finalize_jobs, 1)
        job6 = Job.wrapJobFn(finalize_jobs, 2)
        job7 = Job.wrapJobFn(finalize_jobs, 3)
        job8 = Job.wrapJobFn(finalize_jobs, 4)

        # write files 'A.txt', 'B.txt', and 'C.txt'
        job0.addChild(job1)
        job1.addChild(job2)
        job2.addChild(job3)

        # finally use 'A.txt', 'B.txt', and 'C.txt' to write ABC.txt
        job0.addFollowOn(job4)

        # these jobs do nothing, but should display in status
        job4.addChild(job5)
        job4.addChild(job6)
        job4.addChild(job7)
        job4.addChild(job8)

        toil.start(job0)
