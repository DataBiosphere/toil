from contextlib import contextmanager
import inflection
import logging
import psycopg2
from psycopg2.extras import DictCursor
from uuid import uuid4

from six.moves import cPickle

from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             NoSuchJobException,
                                             NoSuchFileException,
                                             JobStoreExistsException,
                                             NoSuchJobStoreException)
from toil.common import Toil, Config
from toil.jobGraph import JobGraph

import fileStores

logger = logging.getLogger(__name__)

from psycopg2.extensions import adapt, register_adapter, AsIs
register_adapter(dict, psycopg2.extras.Json)

class JobStore(AbstractJobStore):
    """
    A job store that uses postgres database
    """

    def __init__(self, url):
        """
        :param url URI: Postgres DB URL
        """
        super(JobStore, self).__init__()
        self.db_url, filestore_url = url.split('+')
        logger.debug("Connecting to '%s'", self.db_url)
        self.conn = psycopg2.connect(self.db_url, cursor_factory=DictCursor)
        self.fileStore = fileStores.fromUrl(filestore_url)

    def initialize(self, config):
        self._create_jobstore_table()
        self._create_filestore_table()
        self._create_sql_triggers()
        self.conn.commit()

        logger.debug('initialized')
        self.fileStore.initialize(config)
        super(JobStore, self).initialize(config)

    def resume(self):
        with self.conn.cursor() as cur:
            cur.execute("select * from information_schema.tables where table_name=%s", ('job_store',))
            if not bool(cur.rowcount):
                raise NoSuchJobStoreException(self.url)
        super(JobStore, self).resume()
        self.fileStore.resume()
        self.conn.commit()

    def destroy(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute('DROP TABLE IF EXISTS "job_store"')
                cur.execute('DROP TABLE IF EXISTS "file_store"')
            self.fileStore.destroy()
            self.conn.commit()
            logger.debug('Successfully deleted jobStore:%s' % self.db_url)
        except RuntimeError as e:
            # Handle known erros
            self.conn.rollback()
            raise e

    ##########################################
    # The following methods deal with creating/loading/updating/writing/checking for the
    # existence of jobs
    ##########################################

    def create(self, jobNode):
        job = JobGraph.fromJobNode(jobNode, jobStoreID=self._newJobID(), tryCount=self._defaultTryCount())
        try:
            with self.conn.cursor() as cur:
                attrs = job.__dict__
                attrs['predecessorsFinished'] = list(attrs['predecessorsFinished'])
                attrs.pop('_config')
                self._insert_row('job_store', **attrs)
                self.conn.commit()
        except RuntimeError as e:
            # Handle known erros
            self.conn.rollback()
            raise e
        return job

    def exists(self, jobStoreID):
        cur = self.conn.cursor()
        cur.execute("SELECT job_store_id FROM job_store WHERE job_store_id = %s AND _deleted_at IS NULL;", (jobStoreID,))
        self.conn.commit()
        return bool(cur.rowcount)

    def load(self, jobStoreID):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT * FROM job_store WHERE job_store_id = %s AND _deleted_at IS NULL;", (jobStoreID,))
                record = cur.fetchone()
        except RuntimeError as e:
            # Handle known erros
            raise e

        self.conn.commit()
        return self.__jobGraphFromRecord(record)


    def update(self, job):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE job_store
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
                        WHERE job_store_id = %s;
                """, (
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
                    list(job.predecessorsFinished),
                    cPickle.dumps(job.stack),
                    job.services,
                    job.startJobStoreID,
                    job.terminateJobStoreID,
                    job.errorJobStoreID,
                    job.logJobStoreFileID,
                    job.checkpoint,
                    job.checkpointFilesToDelete,
                    cPickle.dumps(job.chainedJobs),
                    job.jobStoreID,
                ))
            self.conn.commit()
        except RuntimeError as e:
            # Handle known erros
            self.conn.rollback()
            raise e

    def delete(self, jobStoreID):
        try:
            with self.conn.cursor() as cur:
                cur.execute( "UPDATE job_store SET _deleted_at = now() WHERE job_store_id = %s AND _deleted_at IS NULL;", (jobStoreID,))
            self.conn.commit()
        except RuntimeError as e:
            # Handle known erros
            self.conn.rollback()
            raise e

    def jobs(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT * FROM job_store WHERE _deleted_at is NULL;")

                for record in cur:
                    yield self.__jobGraphFromRecord(record)
        except RuntimeError as e:
            # Handle known erros
            raise e
        self.conn.commit()

    ##########################################
    # Functions that deal with files
    ##########################################

    # These set of functions are being used in exportFile and importFile
    # functionality. They need to be delegated to pg fileStores. Still
    # no clarity on the potential overlap with toil filestore
    def _readFromUrl(self, *args, **kwargs):
        raise

    def _supportsUrl(self, *args, **kwargs):
        raise

    def _writeToUrl(self, *args, **kwargs):
        raise

    # Seems to be called only from implementations of getSharedPublicUrl
    def getPublicUrl(self, *args, **kwargs):
        raise

    # This is interesting. This is used in Resource.create, which apparently
    # distributes files to all nodes of a cluster.
    # TODO: Test and see if this can be used to our advantage.
    def getSharedPublicUrl(self, *args, **kwargs):
        raise

    # Being used to create FileID
    # TODO: Investigate whether and how filestore is overlapping with jobstore
    @classmethod
    def getSize(cls, *args, **kwargs):
        raise

    # The Only place this seems to be used is in Azure store
    # and jobstore tests
    # $ git grep updateFile | grep -v def | grep -v updateFileStream
    def updateFile(self, *args, **kwargs):
        raise

    def deleteFile(self, jobStoreFileID, force=False):
        try:
            with self.conn.cursor() as cur:
                cur.execute("UPDATE file_store SET _deleted_at = now() WHERE id = %s AND _deleted_at IS NULL;", (jobStoreFileID, ))
                if force and cur.rowcount:
                    self.fileStore.deleteFile(jobStoreFileID)
            self.conn.commit()

        except RuntimeError as e:
            # Handle known erros
            self.conn.rollback()
            raise e

    def fileExists(self, jobStoreFileID):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT id FROM file_store WHERE id = %s AND _deleted_at IS NULL;", (jobStoreFileID, ))
                exists = bool(cur.rowcount)

        except RuntimeError as e:
            # Handle known erros
            raise e

        self.conn.commit()
        return exists

    def getEmptyFileStoreID(self, jobStoreID=None):
        with self.writeFileStream(jobStoreID) as (fileHandle, jobStoreFileID):
            return jobStoreFileID

    def readFile(self, jobStoreFileID, localFilePath):
        self._checkJobStoreFileID(jobStoreFileID)
        self.fileStore.readFile(jobStoreFileID, localFilePath)

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        with self.fileStore.readFileStream(jobStoreFileID) as f:
            yield f

    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT id FROM file_store WHERE shared IS TRUE AND name = %s AND _deleted_at IS NULL;", (sharedFileName, ))
                record = cur.fetchone()
                if record is None:
                    raise NoSuchFileException(sharedFileName,sharedFileName)
                else:
                    with self.fileStore.readFileStream(record['id']) as f:
                        yield f

        except RuntimeError as e:
            # Handle known erros
            raise e
        self.conn.commit()

    def readStatsAndLogging(self, callback, readAll=False):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT id FROM file_store WHERE logs_and_stats IS TRUE AND _deleted_at IS NULL;")
                for record in cur:
                    with self.fileStore.readFileStream(record['id']) as f:
                        try:
                            callback(f)
                        except ValueError as e:
                            logger.debug("log record: %s", record)
                            raise e
                cur.execute("UPDATE file_store SET _deleted_at = now() WHERE logs_and_stats IS TRUE;")

                return cur.rowcount
            self.conn.commit()
        except RuntimeError as e:
            # Handle known erros
            self.conn.rollback()
            raise e

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        with self.fileStore.writeFileStream(jobStoreFileID) as f:
            yield f

    def writeFile(self, localFilePath, jobStoreID=None):
        with self._createJobStoreFileID(jobStoreID=jobStoreID) as fileID:
            self.fileStore.writeFile(localFilePath, fileID)
            return fileID

    @contextmanager
    def writeFileStream(self, *args, **kwargs):
        with self._createJobStoreFileID(*args, **kwargs) as fileID:
            with self.fileStore.writeFileStream(fileID) as (fd):
                yield fd, fileID

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None):
        assert super(JobStore, self)._validateSharedFileName( sharedFileName )
        with self.writeFileStream(name=sharedFileName, shared=True, isProtected=isProtected) as (f, fileID):
            logger.debug("writing shared file %s to %s" % (sharedFileName, fileID))
            yield f

    def writeStatsAndLogging(self, statsAndLoggingString):
        # TODO: parse and log to collectd
        with self.writeFileStream(logsAndStats=True) as (f, fileID):
            logger.debug("Logging stats to %s" % (fileID))
            f.write(statsAndLoggingString)

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
        return str(uuid4())

    def _create_sql_triggers(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    CREATE OR REPLACE FUNCTION update_updated_at_column()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            NEW._updated_at = now();
                            RETURN NEW;
                        END;
                        $$ language 'plpgsql';
                """)
                cur.execute("""
                    do
                    $$
                    declare
                      l_rec record;
                    begin
                      for l_rec in (select table_schema, table_name, column_name
                                    from information_schema.columns
                                    where table_schema = 'public'
                                      and column_name = '_updated_at') loop
                         execute format ('
                            CREATE TRIGGER tg_%I_updated_at
                                BEFORE UPDATE
                                ON %I.%I
                                FOR EACH ROW
                                EXECUTE PROCEDURE update_updated_at_column();
                            ', l_rec.table_name, l_rec.table_schema, l_rec.table_name);
                      end loop;
                    end;
                    $$
                """)
        except RuntimeError as e:
            # Handle known erros
            raise e

    def _create_jobstore_table(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE job_store (
                        job_store_id UUID PRIMARY KEY,
                        command VARCHAR,
                        memory BIGINT,
                        cores INT,
                        disk BIGINT,
                        unit_name VARCHAR,
                        job_name VARCHAR,
                        preemptable BOOLEAN,
                        remaining_retry_count INT,
                        predecessor_number INT,
                        files_to_delete VARCHAR[],
                        predecessors_finished JSONB,
                        stack TEXT,
                        services JSONB,
                        start_job_store_id UUID,
                        terminate_job_store_id UUID,
                        error_job_store_id UUID,
                        log_job_store_file_id UUID,
                        checkpoint BOOLEAN,
                        checkpoint_files_to_delete VARCHAR[],
                        chained_jobs TEXT,
                        _created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
                        _updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
                        _deleted_at TIMESTAMP
                    );
                """)
        except RuntimeError as e:
            # Handle known erros
            raise e

    def _create_filestore_table(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE file_store (
                        id UUID PRIMARY KEY,
                        job_store_id UUID,
                        name VARCHAR,
                        shared BOOLEAN,
                        logs_and_stats BOOLEAN,
                        is_protected BOOLEAN,
                        _created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
                        _updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
                        _deleted_at TIMESTAMP
                    );
                """)
        except RuntimeError as e:
            # Handle known erros
            raise e

    def _insert_row(self, tableName, **fields):
        placeholderList = ','.join("%%(%s)s" % key for key in fields.keys())
        fieldList = ','.join(inflection.underscore(key.strip('_')) for key in fields.keys())
        sqlQuery = "INSERT INTO %s (%s) VALUES (%s);" % (tableName, fieldList, placeholderList)
        try:
            with self.conn.cursor() as cur:
                logger.debug('inserting %s into %s', fields, tableName)
                cur.execute(sqlQuery, fields)
        except RuntimeError as e:
            # Handle known erros
            raise e

    def _checkJobStoreFileID(self, jobStoreFileID):
        """
        :raise NoSuchFileException: if the jobStoreFileID does not exist or is not a file
        """
        if not self.fileExists(jobStoreFileID):
            raise NoSuchFileException("File %s does not exist in jobStore" % jobStoreFileID)

    @contextmanager
    def _createJobStoreFileID(self, jobStoreID=None, jobStoreFileID=None, name=None, shared=None, logsAndStats=None, isProtected=None):
        fileID = jobStoreFileID or str(uuid4())
        self._insert_row('file_store', id=fileID, jobStoreID=jobStoreID, name=name, shared=shared, logsAndStats=logsAndStats, isProtected=isProtected)
        yield fileID
        self.conn.commit()

    def __camelize_keys(self, record):
        return dict((inflection.camelize(k.replace('_id', '_iD'), False), v) for k, v in record.iteritems())

    def __jobGraphFromRecord(self, record):
        attrs = self.__camelize_keys(record)
        attrs['predecessorsFinished'] = set(attrs['predecessorsFinished'])
        attrs['memory'] = int(attrs['memory'])
        attrs['disk'] = int(attrs['disk'])
        attrs['stack'] = cPickle.loads(attrs['stack'])
        attrs['chainedJobs'] = cPickle.loads(attrs['chainedJobs'])
        attrs.pop('_createdAt')
        attrs.pop('_updatedAt')
        attrs.pop('_deletedAt')
        return JobGraph(**attrs)
