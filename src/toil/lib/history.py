# Copyright (C) 2024 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Contains tools for tracking history.
"""

import logging
import os
import sqlite3
import threading
import uuid
from typing import Any, Literal, Optional, Union, TypedDict, cast

from toil.lib.io import get_toil_home

logger = logging.getLogger(__name__)

class HistoryDatabaseSchemaTooNewError(RuntimeError):
    """
    Raised when we would write to the history database, but its schema is too
    new for us to understand.
    """
    pass

class HistoryManager:
    """
    Class responsible for managing the history of Toil runs.
    """

    @classmethod
    def database_path(cls) -> str:
        """
        Get the path at which the database we store history in lives.
        """
        return os.path.join(get_toil_home(), "history.sqlite")

    @classmethod
    def connection(cls) -> sqlite3.Connection:
        """
        Connect to the history database.

        Caller must not actually use the connection without using
        ensure_tables() to protect reads and updates.
        """
        con = sqlite3.connect(
            cls.database_path()
        )
        # This doesn't much matter given the default isolation level settings,
        # but is recommended.
        con.autocommit = False
        # We use foreign keys
        con.execute("PRAGMA foreign_keys = ON")
        return con

    @classmethod
    def ensure_tables(cls, con: sqlite3.Connection, cur: sqlite3.Cursor) -> None:
        """
        Ensure that tables exist in the database and the schema is migrated to the current version.

        Leaves the cursor in a transaction where the schema version is known to be correct.

        :raises HistoryDatabaseSchemaTooNewError: If the schema is newer than the current version.
        """
        
        # Python already puts us in a transaction.
        
        # TODO: Do a try-and-fall-back to avoid sending the table schema for
        # this every time we do anything.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS migrations (
                version INT NOT NULL PRIMARY KEY,
                description TEXT
            )
        """)
        db_version = next(cur.execute("SELECT MAX(version) FROM migrations"))[0]
        if db_version is None:
            db_version = -1

        # This holds pairs of description and command lists.
        # To make a schema change, ADD A NEW PAIR AT THE END, and include
        # statements to adjust existing data.
        migrations = [
            (
                "Make initial tables",
                [
                    """
                    CREATE TABLE workflows (
                        id TEXT NOT NULL PRIMARY KEY,
                        jobstore TEXT NOT NULL,
                        name TEXT
                    )
                    """,
                    """
                    CREATE TABLE job_attempts (
                        id TEXT NOT NULL PRIMARY KEY,
                        workflow_id TEXT NOT NULL,
                        workflow_attempt_number INT NOT NULL,
                        job_name TEXT NOT NULL,
                        succeeded INTEGER NOT NULL,
                        start_time REAL NOT NULL,
                        runtime REAL NOT NULL,
                        FOREIGN KEY(workflow_id) REFERENCES workflows(id)
                    )
                    """,
                    """
                    CREATE TABLE workflow_attempts (
                        workflow_id TEXT NOT NULL,
                        attempt_number INTEGER NOT NULL,
                        succeeded INTEGER NOT NULL,
                        start_time REAL NOT NULL,
                        runtime REAL NOT NULL,
                        PRIMARY KEY(workflow_id,attempt_number),
                        FOREIGN KEY(workflow_id) REFERENCES workflows(id)
                    )
                    """
                ]
            ),
        ]

        if db_version + 1 > len(migrations):
            raise HistoryDatabaseSchemaTooNewError(f"History database version is {db_version}, but known migrations only go up to {len(migrations) - 1}")

        for migration_number in range(db_version + 1, len(migrations)):
            for statement_number, statement in enumerate(migrations[migration_number][1]):
                # Run all the migration commands.
                # We don't use executescript() because (on old Pythons?) it
                # commits the current transactrion first.
                try:
                    cur.execute(statement)
                except sqlite3.OperationalError:
                    logger.exception("Could not execute migration %s statement %s: %s", migration_number, statement_number, statement)
                    raise
            cur.execute("INSERT INTO migrations VALUES (?, ?)", (migration_number, migrations[migration_number][0]))

        # If we did have to migrate, leave everything else we do as part of the migration transaction.
        
    @classmethod
    def record_workflow_creation(cls, workflow_id: str, job_store_spec: str) -> None:
        """
        Record that a workflow is being run.

        Takes the Toil config's workflow ID and the location of the job store.

        Should only be called on the *first* attempt on a job store, not on a
        restart.

        A workflow may have multiple attempts to run it, some of which succeed
        and others of which fail. Probably only the last one should succeed.
        """
        
        logger.info("Recording workflow creation of %s in %s", workflow_id, job_store_spec)

        con = cls.connection()
        cur = con.cursor()
        try:
            cls.ensure_tables(con, cur)
            cur.execute("INSERT INTO workflows VALUES (?, ?, NULL)", (workflow_id, job_store_spec))
        except:
            con.rollback()
            raise
        else:
            con.commit()

        # If we raise out of here the connection goes away and the transaction rolls back.


    @classmethod
    def record_workflow_metadata(cls, workflow_id: str, workflow_name: str) -> None:
        """
        Associate a name (possibly a TRS ID and version string) with a workflow run.
        """

        # TODO: Make name of this function less general?

        logger.info("Workflow %s is a run of %s", workflow_id, workflow_name)

        con = cls.connection()
        cur = con.cursor()
        try:
            cls.ensure_tables(con, cur)
            cur.execute("UPDATE workflows SET name = ? WHERE id = ?", (workflow_name, workflow_id))
        except:
            con.rollback()
            raise
        else:
            con.commit()

    @classmethod
    def record_job_attempt(cls, workflow_id: str, workflow_attempt_number: int, job_name: str, succeeded: bool, start_time: float, runtime: float) -> None:
        """
        Record that a job ran in a workflow.

        Doesn't expect the provided information to uniquely identify the job attempt; assigns the job attempt its own unique ID.

        Thread safe.
        
        :param job_name: A human-readable name for the job. Not expected to be
            a job store ID or to necessarily uniquely identify the job within
            the workflow.
        :param start_time: Job execution start time ins econds since epoch.
        :param runtime: Job execution duration in seconds.
        """

        logger.info("Workflow %s ran job %s", workflow_id, job_name)

        con = cls.connection()
        cur = con.cursor()
        try:
            cls.ensure_tables(con, cur)
            cur.execute(
                "INSERT INTO job_attempts VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    str(uuid.uuid4()),
                    workflow_id,
                    workflow_attempt_number,
                    job_name,
                    1 if succeeded else 0,
                    start_time,
                    runtime
                )
            )
        except:
            con.rollback()
            raise
        else:
            con.commit()

    # TODO: Should we force workflow attempts to be reported on start so that we can have the jobs key-reference them?

    @classmethod
    def record_workflow_attempt(cls, workflow_id: str, workflow_attempt_number: int, succeeded: bool, start_time: float, runtime: float) -> None:
        """
        Record a workflow attempt (start or restart) having finished or failed.
        """

        logger.info("Workflow %s stopped. Success: %s", workflow_id, succeeded)

        con = cls.connection()
        cur = con.cursor()
        try:
            cls.ensure_tables(con, cur)
            cur.execute(
                "INSERT INTO workflow_attempts VALUES (?, ?, ?, ?, ?)",
                (
                    workflow_id,
                    workflow_attempt_number,
                    1 if succeeded else 0,
                    start_time,
                    runtime
                )
            )
        except:
            con.rollback()
            raise
        else:
            con.commit()

   
