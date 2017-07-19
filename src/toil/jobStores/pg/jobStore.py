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
        if '#' in self.db_url:
            self.db_url, self.namespace = self.db_url.split('#')
        else:
            self.namespace = 'default'
        self.conn = psycopg2.connect(self.db_url, cursor_factory=DictCursor)
        self.fileStore = fileStores.fromUrl(filestore_url)

        self._setup_db()


    def initialize(self, config):
        logger.debug('initialized')
        self.fileStore.initialize(config)
        super(JobStore, self).initialize(config)

    def resume(self):
        with self.conn.cursor() as cur:
            cur.execute("select * from job_store where namespace=%s", (self.namespace,))
            if not bool(cur.rowcount):
                raise NoSuchJobStoreException(self.url)
            cur.execute("select * from file_store where namespace=%s", (self.namespace,))
            if not bool(cur.rowcount):
                raise NoSuchJobStoreException(self.url)
        super(JobStore, self).resume()
        self.fileStore.resume()
        self.conn.commit()

    def destroy(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute('DELETE FROM job_store WHERE namespace = %s', (self.namespace, ))
                cur.execute('DELETE FROM file_store WHERE namespace = %s', (self.namespace, ))
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
        cur.execute("SELECT job_store_id FROM job_store WHERE namespace = %s AND job_store_id = %s AND _deleted_at IS NULL;", (self.namespace, jobStoreID))
        self.conn.commit()
        return bool(cur.rowcount)

    def load(self, jobStoreID):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT * FROM job_store WHERE namespace = %s AND job_store_id = %s AND _deleted_at IS NULL;", (self.namespace, jobStoreID))
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
                        WHERE
                            namespace = %s AND
                            job_store_id = %s;
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
                    self.namespace,
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
                cur.execute( "UPDATE job_store SET _deleted_at = now() WHERE namespace = %s AND job_store_id = %s AND _deleted_at IS NULL;", (self.namespace, jobStoreID))
            self.conn.commit()
        except RuntimeError as e:
            # Handle known erros
            self.conn.rollback()
            raise e

    def jobs(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT * FROM job_store WHERE namespace = %s AND _deleted_at is NULL;", (self.namespace, ))

                for record in cur:
                    yield self.__jobGraphFromRecord(record)
        except RuntimeError as e:
            # Handle known erros
            raise e
        self.conn.commit()

    ##########################################
    # Functions that deal with file urls
    ##########################################

    # These set of functions are being used in exportFile and importFile
    # functionality. They need to be delegated to pg fileStores. Still
    # no clarity on the potential overlap with toil filestore
    @classmethod
    def _supportsUrl(cls, url, export=False):
        try:
            return fileStores.getStore(url).supportsUrl(url, export)
        except NoSupportedFileStoreException:
            return False

    @classmethod
    def _readFromUrl(cls, url, writable):
        fileStores.getStore(url).readFromUrl(url, writable)

    @classmethod
    def _writeToUrl(cls, readable, url):
        fileStores.getStore(url).writeToUrl(readable, url)

    # Being used to create FileID
    # TODO: Investigate whether and how filestore is overlapping with jobstore
    @classmethod
    def getSize(cls, url):
        raise

    # This is interesting. This is used in Resource.create, which apparently
    # distributes files to all nodes of a cluster.
    # TODO: Test and see if this can be used to our advantage.
    def getSharedPublicUrl(self, sharedFileName):
        fileID = self._getSharedFileID(sharedFileName)
        return self.getPublicUrl(fileID)

    # Seems to be called only from implementations of getSharedPublicUrl
    def getPublicUrl(self, jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        self.fileStore.getPublicUrl(jobStoreFileID)

    ##########################################
    # Functions that deal with files
    ##########################################

    def fileExists(self, jobStoreFileID):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT id FROM file_store WHERE namespace = %s AND id = %s AND _deleted_at IS NULL;", (self.namespace, jobStoreFileID))
                exists = bool(cur.rowcount)

        except RuntimeError as e:
            # Handle known erros
            raise e

        self.conn.commit()
        return exists

    def readFile(self, jobStoreFileID, localFilePath):
        self._checkJobStoreFileID(jobStoreFileID)
        self.fileStore.readFile(jobStoreFileID, localFilePath)

    def writeFile(self, localFilePath, jobStoreID=None):
        with self._createJobStoreFileID(jobStoreID=jobStoreID) as fileID:
            self.fileStore.writeFile(localFilePath, fileID)
            return fileID

    # The Only place this seems to be used is in Azure store
    # and jobstore tests
    # $ git grep updateFile | grep -v def | grep -v updateFileStream
    def updateFile(self, *args, **kwargs):
        raise

    def deleteFile(self, jobStoreFileID, force=False):
        try:
            with self.conn.cursor() as cur:
                cur.execute("UPDATE file_store SET _deleted_at = now() WHERE namespace = %s AND id = %s AND _deleted_at IS NULL;", (self.namespace, jobStoreFileID))
                if force and cur.rowcount:
                    self.fileStore.deleteFile(jobStoreFileID)
            self.conn.commit()

        except RuntimeError as e:
            # Handle known erros
            self.conn.rollback()
            raise e

    ##########################################
    # Functions that deal with file streams
    ##########################################

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        with self.fileStore.readFileStream(jobStoreFileID) as f:
            yield f

    @contextmanager
    def writeFileStream(self, *args, **kwargs):
        with self._createJobStoreFileID(*args, **kwargs) as fileID:
            with self.fileStore.writeFileStream(fileID) as (fd):
                yield fd, fileID

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        with self.fileStore.writeFileStream(jobStoreFileID) as f:
            yield f

    ##########################################
    # Utility functions that deal with file store
    ##########################################

    def getEmptyFileStoreID(self, jobStoreID=None):
        with self.writeFileStream(jobStoreID) as (fileHandle, jobStoreFileID):
            return jobStoreFileID

    ##########################################
    # Functions that deal with shared file streams
    ##########################################

    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        fileID = self._getSharedFileID(sharedFileName)
        with self.fileStore.readFileStream(fileID) as f:
            yield f

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None):
        assert super(JobStore, self)._validateSharedFileName( sharedFileName )
        with self.writeFileStream(name=sharedFileName, shared=True, isProtected=isProtected) as (f, fileID):
            logger.debug("writing shared file %s to %s" % (sharedFileName, fileID))
            yield f

    ##########################################
    # Functions that deal with stats and logs
    # TODO: Move this out to it's own abstraction
    ##########################################

    def readStatsAndLogging(self, callback, readAll=False):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT id FROM file_store WHERE namespace = %s AND logs_and_stats IS TRUE AND _deleted_at IS NULL;", (self.namespace, ))
                results = cur.fetchall()
                for record in results:
                    with self.fileStore.readFileStream(record[0]) as f:
                        try:
                            callback(f)
                        except ValueError as e:
                            logger.debug("ValueError for log record: %s", record)
                            raise e
                    cur.execute("UPDATE file_store SET _deleted_at = now() WHERE namespace = %s AND id = %s;", (self.namespace, record[0]))

            self.conn.commit()
            return len(results)
        except RuntimeError as e:
            # Handle known erros
            self.conn.rollback()
            raise e

    def writeStatsAndLogging(self, statsAndLoggingString):
        # TODO: parse and log to collectd
        with self.writeFileStream(logsAndStats=True) as (f, fileID):
            logger.debug("Logging stats to %s" % (fileID))
            f.write(statsAndLoggingString)

    ##########################################
    # Private methods
    ##########################################

    def _newJobID(self):
        return str(uuid4())

    def _insert_row(self, tableName, **fields):
        fields['namespace'] = self.namespace
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

    def _getSharedFileID(self, sharedFileName):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT id FROM file_store WHERE namespace = %s AND shared IS TRUE AND name = %s AND _deleted_at IS NULL;", (self.namespace, sharedFileName))
                record = cur.fetchone()
                if record is None:
                    raise NoSuchFileException(sharedFileName, sharedFileName)
                else:
                    return record['id']

        except RuntimeError as e:
            # Handle known erros
            raise e
        self.conn.commit()


    @contextmanager
    def _createJobStoreFileID(self, jobStoreID=None, jobStoreFileID=None, name=None, shared=None, logsAndStats=None, isProtected=None):
        fileID = jobStoreFileID or str(uuid4())
        self._insert_row('file_store', id=fileID, jobStoreID=jobStoreID, name=name, shared=shared, logsAndStats=logsAndStats, isProtected=isProtected)
        yield fileID
        self.conn.commit()

    def __sanitize_record(self, record):
        attrs = self.__camelize_keys(record)
        attrs.pop('_createdAt')
        attrs.pop('_updatedAt')
        attrs.pop('_deletedAt')
        attrs.pop('namespace')
        return attrs

    def __camelize_keys(self, record):
        return dict((inflection.camelize(k.replace('_id', '_iD'), False), v) for k, v in record.iteritems())

    def __jobGraphFromRecord(self, record):
        attrs = self.__sanitize_record(record)
        attrs['predecessorsFinished'] = set(attrs['predecessorsFinished'])
        attrs['memory'] = int(attrs['memory'])
        attrs['disk'] = int(attrs['disk'])
        attrs['stack'] = cPickle.loads(attrs['stack'])
        attrs['chainedJobs'] = cPickle.loads(attrs['chainedJobs'])
        return JobGraph(**attrs)

    def _setup_db(self):
        self._create_jobstore_table()
        self._create_filestore_table()
        self._create_sql_triggers()
        self.conn.commit()
        logger.debug('Finished setting up DB')

    def _create_jobstore_table(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS job_store (
                        namespace VARCHAR,
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
            logger.debug('Created job_store table')
        except RuntimeError as e:
            # Handle known erros
            raise e

    def _create_filestore_table(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS file_store (
                        namespace VARCHAR,
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
            logger.debug('Created file_store table')
        except RuntimeError as e:
            # Handle known erros
            raise e

    # NOTE: The embedded PL/pgSQL script is playing havoc with atom syntax highlighter.
    #       Keep this function at the end to limit the pain.
    def _create_sql_triggers(self):
        try:
            with self.conn.cursor() as cur:
                function_plsql = """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'set_updated_at_column') THEN
                            CREATE FUNCTION set_updated_at_column()
                                RETURNS TRIGGER AS $func$
                                BEGIN
                                    NEW._updated_at = now();
                                    RETURN NEW;
                                END;
                                $func$ language 'plpgsql';
                        END IF;
                    END
                    $$;
                """
                trigger_plsql = """
                    DO
                    $do$
                    DECLARE
                       _tbl text;
                       _trg text;
                    BEGIN
                    FOR _tbl, _trg IN
                       SELECT c.oid::regclass::text, 'tg_' || relname || '_set_updated_at'
                       FROM   pg_class        c
                       JOIN   pg_namespace    n ON n.oid = c.relnamespace
                       LEFT   JOIN pg_trigger t ON t.tgname = 'tg_' || c.relname || '_set_updated_at'
                                               AND t.tgrelid = c.oid  -- check only respective table
                       WHERE  n.nspname = 'public'
                       AND    c.relkind = 'r'   -- only regular tables
                       AND    t.tgrelid IS NULL -- trigger does not exist yet
                    LOOP
                       EXECUTE format(
                          'CREATE TRIGGER %I
                           BEFORE UPDATE ON %s
                           FOR EACH ROW EXECUTE PROCEDURE set_updated_at_column()'
                         , _trg, _tbl::text
                       );
                    END LOOP;
                    END
                    $do$;
                """
                cur.execute(function_plsql)
                cur.execute(trigger_plsql)
            logger.debug('Created updated_at triggers')
        except RuntimeError as e:
            # Handle known erros
            raise e
