import psycopg2

from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             NoSuchJobException,
                                             NoSuchFileException,
                                             JobStoreExistsException,
                                             NoSuchJobStoreException)

class PGJobStore(AbstractJobStore):
    """
    A job store that uses postgres database
    """

    tableName = 'job_store'

    def __init__(self, url):
        """
        :param url URI: Postgres DB URL
        """
        super(PGJobStore, self).__init__()
        db_url, filestore_url = url.split('+')
        logger.debug("Connecting to '%s'", url)
        self.db_url = url
        self.fileStore = Toil.getJobStore(filestore_url)
        self.conn = psycopg2.connect(url)

    def initialize(self, config):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE %s (
                        id UUID PRIMARY KEY,
                        command VARCHAR,
                        memory INT,
                        cores INT,
                        disk INT,
                        unit_name VARCHAR,
                        job_name VARCHAR,
                        preemptable BOOLEAN,
                        remaining_retry_count INT,
                        predecessor_number INT,
                        files_to_delete JSONB,
                        predecessors_finished JSONB,
                        stack JSONB,
                        services JSONB,
                        start_job_store_id UUID,
                        terminate_job_store_id UUID,
                        error_job_store_id UUID,
                        log_job_store_file_id UUID,
                        checkpoint BOOLEAN,
                        checkpoint_files_to_delete JSONB,
                        chained_jobs JSONB
                    );
                """, (self.tableName, ))
        except RuntimeError as e:
            # Handle known erros
            raise
        super(PGJobStore, self).initialize(config)

    def resume(self):
        with self.conn.cursor() as cur:
            cur.execute("select * from information_schema.tables where table_name=%s", (self.tableName,))
            if not bool(cur.rowcount):
                raise NoSuchJobStoreException(self.url)
        super(PGJobStore, self).resume()

    def destroy(self):
        with self.conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS "%s"', (self.tableName,))
        super(PGJobStore, self).destroy()

    ##########################################
    # The following methods deal with creating/loading/updating/writing/checking for the
    # existence of jobs
    ##########################################

    def create(self, jobNode):
        job = JobGraph.fromJobNode(jobNode, jobStoreID=self._newJobID(), tryCount=self._defaultTryCount())
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO %s (
                        id,
                        command,
                        memory,
                        cores,
                        disk,
                        unit_name,
                        job_name,
                        preemptable,
                        remaining_retry_count,
                        predecessor_number,
                        files_to_delete,
                        predecessors_finished,
                        stack,
                        services,
                        start_job_store_id,
                        terminate_job_store_id,
                        error_job_store_id,
                        log_job_store_file_id,
                        checkpoint,
                        checkpoint_files_to_delete,
                        chained_jobs
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s
                    );
                """, (
                    self.tableName,
                    job.jobStoreID,
                    job.command,
                    job._memory,
                    job._cores,
                    job._disk,
                    job.unitName,
                    job.jobName,
                    job._preemptable,
                    job.remainingRetryCount,
                    job.predecessorNumber,
                    job.filesToDelete,
                    job.predecessorsFinished,
                    job.stack,
                    job.services,
                    job.startJobStoreID,
                    job.terminateJobStoreID
                    job.errorJobStoreID,
                    job.logJobStoreFileID,
                    job.checkpoint,
                    job.checkpointFilesToDelete,
                    job.chainedJobs,
                ))
        except RuntimeError as e:
            # Handle known erros
            raise
        return job

    def exists(self, jobStoreID):
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM %s WHERE id = %s", (self.tableName, track_id,))
        return cur.fetchone() is not None

    def load(self, jobStoreID):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        command,
                        memory,
                        cores,
                        disk,
                        unit_name,
                        job_name,
                        preemptable,
                        remaining_retry_count,
                        predecessor_number,
                        files_to_delete,
                        predecessors_finished,
                        stack,
                        services,
                        start_job_store_id,
                        terminate_job_store_id,
                        error_job_store_id,
                        log_job_store_file_id,
                        checkpoint,
                        checkpoint_files_to_delete,
                        chained_jobs
                    FROM %s
                    WHERE id = %s;
                """, (
                    self.tableName,
                    jobStoreID
                ))

                (
                    command,
                    memory,
                    cores,
                    disk,
                    unitName,
                    jobName,
                    preemptable,
                    remainingRetryCount,
                    predecessorNumber,
                    filesToDelete,
                    predecessorsFinished,
                    stack,
                    services,
                    startJobStoreID,
                    terminateJobStoreID,
                    errorJobStoreID,
                    logJobStoreFileID,
                    checkpoint,
                    checkpointFilesToDelete,
                    chainedJobs
                ) = cur.fetchone()
        except RuntimeError as e:
            # Handle known erros
            raise

        return JobGraph(
            command = command,
            memory = memory,
            cores = cores,
            disk = disk,
            unitName = unitName,
            jobName = jobName,
            preemptable = preemptable,
            jobStoreID = jobStoreID,
            remainingRetryCount = remainingRetryCount,
            predecessorNumber = predecessorNumber,
            filesToDelete = filesToDelete,
            predecessorsFinished = predecessorsFinished,
            stack = stack,
            services = services,
            startJobStoreID = startJobStoreID,
            terminateJobStoreID = terminateJobStoreID,
            errorJobStoreID = errorJobStoreID,
            logJobStoreFileID = logJobStoreFileID,
            checkpoint = checkpoint,
            checkpointFilesToDelete = checkpointFilesToDelete,
            chainedJob = chainedJobs
        )


    def update(self, job):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE %s
                        SET
                            command = %s,
                            memory = %s,
                            cores = %s,
                            disk = %s,
                            unit_name = %s,
                            job_name = %s,
                            preemptable = %s,
                            remaining_retry_count = %s,
                            predecessor_number = %s,
                            files_to_delete = %s,
                            predecessors_finished = %s,
                            stack = %s,
                            services = %s,
                            start_job_store_id = %s,
                            terminate_job_store_id = %s,
                            error_job_store_id = %s,
                            log_job_store_file_id = %s,
                            checkpoint = %s,
                            checkpoint_files_to_delete = %s,
                            chained_jobs = %s
                        WHERE id = %s;
                """, (
                    self.tableName,
                    job.command,
                    job._memory,
                    job._cores,
                    job._disk,
                    job.unitName,
                    job.jobName,
                    job._preemptable,
                    job.remainingRetryCount,
                    job.predecessorNumber,
                    job.filesToDelete,
                    job.predecessorsFinished,
                    job.stack,
                    job.services,
                    job.startJobStoreID,
                    job.terminateJobStoreID
                    job.errorJobStoreID,
                    job.logJobStoreFileID,
                    job.checkpoint,
                    job.checkpointFilesToDelete,
                    job.chainedJobs,
                    job.jobStoreID,
                ))
        except RuntimeError as e:
            # Handle known erros
            raise

    def delete(self, jobStoreID):
        try:
            with self.conn.cursor() as cur:
                cur.execute( "DELETE FROM %s WHERE id = %s;", (self.tableName, jobStoreID,))
        except RuntimeError as e:
            # Handle known erros
            raise

    def jobs(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        command,
                        memory,
                        cores,
                        disk,
                        unit_name,
                        job_name,
                        preemptable,
                        remaining_retry_count,
                        predecessor_number,
                        files_to_delete,
                        predecessors_finished,
                        stack,
                        services,
                        start_job_store_id,
                        terminate_job_store_id,
                        error_job_store_id,
                        log_job_store_file_id,
                        checkpoint,
                        checkpoint_files_to_delete,
                        chained_jobs
                    FROM %s;
                """, (
                    self.tableName,
                ))

                for (
                    command,
                    memory,
                    cores,
                    disk,
                    unitName,
                    jobName,
                    preemptable,
                    remainingRetryCount,
                    predecessorNumber,
                    filesToDelete,
                    predecessorsFinished,
                    stack,
                    services,
                    startJobStoreID,
                    terminateJobStoreID,
                    errorJobStoreID,
                    logJobStoreFileID,
                    checkpoint,
                    checkpointFilesToDelete,
                    chainedJobs
                ) in cur:
                    yield JobGraph(
                        command = command,
                        memory = memory,
                        cores = cores,
                        disk = disk,
                        unitName = unitName,
                        jobName = jobName,
                        preemptable = preemptable,
                        jobStoreID = jobStoreID,
                        remainingRetryCount = remainingRetryCount,
                        predecessorNumber = predecessorNumber,
                        filesToDelete = filesToDelete,
                        predecessorsFinished = predecessorsFinished,
                        stack = stack,
                        services = services,
                        startJobStoreID = startJobStoreID,
                        terminateJobStoreID = terminateJobStoreID,
                        errorJobStoreID = errorJobStoreID,
                        logJobStoreFileID = logJobStoreFileID,
                        checkpoint = checkpoint,
                        checkpointFilesToDelete = checkpointFilesToDelete,
                        chainedJob = chainedJobs
                    )
        except RuntimeError as e:
            # Handle known erros
            raise




    ##########################################
    # Delegated functions that deal with files
    ##########################################

    # def getPublicUrl(self, jobStoreFileID):
    #     return self.fileStore.getPublicUrl(jobStoreFileID)
    #
    # def getSharedPublicUrl(self, sharedFileName):
    #     return self.fileStore.getPublicUrl(sharedFileName)
    #
    # ##########################################
    # # Functions that deal with temporary files associated with jobs
    # ##########################################
    #
    # def _importFile(self, otherCls, url, sharedFileName=None):
    #     return self.fileStore._importFile(otherCls, url, sharedFileName)
    #
    # def _exportFile(self, otherCls, jobStoreFileID, url):
    #     return self.fileStore._exportFile(otherCls, jobStoreFileID, url)

    ##########################################
    # Private methods
    ##########################################

    def _newJobID(self):
        return str(uuid.uuid4())
