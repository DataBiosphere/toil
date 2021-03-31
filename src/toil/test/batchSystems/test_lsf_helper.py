"""lsfHelper.py shouldn't need a batch system and so the unit tests here should aim to run on any system."""
from toil.batchSystems.lsfHelper import parse_mem_and_cmd_from_output
from toil.test import ToilTest


class LSFHelperTest(ToilTest):
    def test_parse_mem_and_cmd_from_output(self):
        # https://github.com/DataBiosphere/toil/pull/3475
        output = ('\nJob <2924748>, Job Name <toil_job_64>, User <thiagogenez>, Project <default>, S'
                  '\n                     tatus <RUN>, Queue <research-rh74>, Job Priority <50>, Com'
                  '\n                     mand <_toil_worker CactusBarRecursion file:/hps/nobackup/p'
                  '\n                     roduction/ensembl/thiagogenez/pairwises/arabidopsis/run/jo'
                  '\n                     bstore/3 kind-CactusBarRecursion/instance-iu6wo56x --conte'
                  '\n                     xt gAShortenedh32xqlE51Yi4=>, Share group charged </thiago'
                  '\n                     genez>, Esub <esub>'
                  '\nThu Mar 18 02:06:32: Submitted from host <noah-login-01>, CWD </hps/nobackup/pr'
                  '\n                     oduction/ensembl/thiagogenez/pairwises/arabidopsis/run>, S'
                  '\n                     pecified CWD </hps/nobackup/production/ensembl/thiagogenez'
                  '\n                     /pairwises/arabidopsis/run/.>, Output File </tmp/toil_work'
                  '\n                     flow_10e83102-2e4b-4093-9128-2a52f4bbc65a_job_64_batch_lsf'
                  '\n                     _2924748_std_output.log>, Error File </tmp/toil_workflow_1'
                  '\n                     0e83102-2e4b-4093-9128-2a52f4bbc65a_job_64_batch_lsf_29247'
                  '\n                     48_std_error.log>, Requested Resources <select[(mem>4000)]'
                  '\n                      rusage[mem=4000:duration=480, numcpus=1:duration=480]>;'
                  '\nThu Mar 18 02:06:33: Started on <hx-noah-31-02>, Execution Home </homes/thiagog'
                  '\n                     enez>, Execution CWD </hps/nobackup/production/ensembl/thi'
                  '\n                     agogenez/pairwises/arabidopsis/run/.>;'
                  '\nThu Mar 18 17:07:47: Resource usage collected.'
                  '\n                     The CPU time used is 53936 seconds.'
                  '\n                     MEM: 344 Mbytes;  SWAP: 1.3 Gbytes;  NTHREAD: 5'
                  '\n                     PGID: 433168;  PIDs: 433168 433177 433179 444026 '
                  '\n'
                  '\n RUNLIMIT                '
                  '\n 10085.0 min'
                  '\n'
                  '\n CORELIMIT MEMLIMIT'
                  '\n      0 M     3.9 G '
                  '\n'
                  '\n MEMORY USAGE:'
                  '\n MAX MEM: 2.5 Gbytes;  AVG MEM: 343 Mbytes'
                  '\n'
                  '\n SCHEDULING PARAMETERS:'
                  '\n           r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem'
                  '\n loadSched   -     -     -     -    10.0     -    -     -   500M     -   1000M '
                  '\n loadStop    -     -     -     -       -     -    -     -     -      -      -  '
                  '\n'
                  '\n          availcpus '
                  '\n loadSched      1.0 '
                  '\n loadStop        -  '
                  '\n'
                  '\n RESOURCE REQUIREMENT DETAILS:'
                  '\n Combined: select[((mem>4000)) && (type == local)] order[r15s:pg] rusage[mem=40'
                  '\n                     00.00:duration=8h:decay=0,numcpus=1.00:duration=8h:decay=0'
                  '\n                     ] span[hosts=1]'
                  '\n Effective: select[(((mem>4000))) && (type == local)] order[r15s:pg] rusage[mem'
                  '\n                     =4000.00:duration=8h:decay=0,numcpus=1.00:duration=8h:deca'
                  '\n                     y=0] span[hosts=1] '
                  '\n'
                  '\n')
        max_mem, command = parse_mem_and_cmd_from_output(output=output)
        assert len(max_mem.groups()) == 1
        expected_mem = '2.5 Gbytes'
        assert max_mem.group(1) == expected_mem, f'Actual: {max_mem.group(1)}, Expected: "{expected_mem}"'

        assert len(command.groups()) == 1
        expected_command = ('_toil_worker CactusBarRecursion file:/hps/nobackup/production/ensembl/thiagogenez/'
                            'pairwises/arabidopsis/run/jobstore/3 kind-CactusBarRecursion/instance-iu6wo56x '
                            '--context gAShortenedh32xqlE51Yi4=')
        assert command.group(1) == expected_command, f'Actual: {command.group(1)}, Expected: "{expected_command}"'
        print(command)

        max_mem, command = parse_mem_and_cmd_from_output(output='')
        assert max_mem == None
        assert command == None
