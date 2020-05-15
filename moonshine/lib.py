import logging
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from alembic.config import Config
from sqlalchemy import engine_from_config, create_engine
from sqlalchemy.engine import Engine
import os, sys

from sqlalchemy import engine_from_config
from sqlalchemy import pool


class Moonshine:
    __config = None
    __engine = None
    __script_directory = None
    __environment_context = None

    def __init__(self, config=None, engine=None, engine_config=None):
        if config is not None:
            assert isinstance(
                config, Config
            ), "config is not an instance of alembic.config.Config"
            self.__config = config
        if engine is not None:
            assert isinstance(
                engine, Engine
            ), "engine is not an instance on sqlalchemy.engine.Engine"
            self.__engine = engine
        elif engine_config is not None:
            self.__engine = engine_from_config(engine_config)

        # set defult values for script_location
        script_location = "migrations"
        self.config.set_main_option("script_location", script_location)
        # set defult values for version_locations
        version_locations = [script_location]
        self.config.set_main_option("version_locations", ",".join(version_locations))

        # add logging handler if not configured
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.formatter = logging.Formatter(
            fmt="%(levelname)-5.5s [%(name)s] %(message)s", datefmt="%H:%M:%S"
        )

        sqlalchemy_logger = logging.getLogger("sqlalchemy")
        alembic_logger = logging.getLogger("alembic")

        if not sqlalchemy_logger.hasHandlers():
            sqlalchemy_logger.setLevel(logging.WARNING)
            sqlalchemy_logger.addHandler(console_handler)

        # alembic adds a null handler, remove it
        if len(alembic_logger.handlers) == 1 and isinstance(
            alembic_logger.handlers[0], logging.NullHandler
        ):
            alembic_logger.removeHandler(alembic_logger.handlers[0])

        if not alembic_logger.hasHandlers():
            alembic_logger.setLevel(logging.INFO)
            alembic_logger.addHandler(console_handler)

    @property
    def config(self) -> Config:
        if isinstance(self.__config, Config):
            return self.__config
        self.__config = Config()
        return self.__config

    @property
    def script_directory(self) -> ScriptDirectory:
        if isinstance(self.__script_directory, ScriptDirectory):
            return self.__script_directory
        self.__script_directory = ScriptDirectory()
        return self.__script_directory

    @property
    def engine(self):
        if isinstance(self.__engine, Engine):
            return self.__engine

        self.__engine = create_engine()
        return self.__engine

    @property
    def environment_context(self) -> EnvironmentContext:
        if isinstance(self.__environment_context, EnvironmentContext):
            return self.__environment_context
        self.__environment_c__environment_contextontect = EnvironmentContext()
        return self.__environment_context

    @property
    def migration_context(self):
        with self.engine.connect() as conn:
            env = self.environment_context
            env.configure(connection=conn)
            yield env

    def run_migrations(self, fn, **kwargs):
        """Configure an Alembic :class:`~alembic.runtime.migration.MigrationContext` to run migrations for the given function.

        This takes the place of Alembic's env.py file, specifically the ``run_migrations_online`` function.

        :param fn: use this function to control what migrations are run
        :param kwargs: extra arguments passed to ``upgrade`` or ``downgrade`` in each revision
        """

        env = self.environment_context

        with self.engine.connect() as connection:
            env.configure(
                connection=connection, fn=fn,
            )

            with env.begin_transaction():
                env.run_migrations(**kwargs)

    def current(self):
        """Get the list of current revisions."""

        return self.script_directory.get_revisions(
            self.migration_context.get_current_heads()
        )

    def heads(self, resolve_dependencies=False):
        """Get the list of revisions that have no child revisions.

        :param resolve_dependencies: treat dependencies as down revisions
        """

        if resolve_dependencies:
            return self.script_directory.get_revisions("heads")

        return self.script_directory.get_revisions(self.script_directory.get_heads())

    def branches(self):
        """Get the list of revisions that have more than one next revision."""

        return [
            revision
            for revision in self.script_directory.walk_revisions()
            if revision.is_branch_point
        ]

    def log(self, start="base", end="heads"):
        """Get the list of revisions in the order they will run.

        :param start: only get since this revision
        :param end: only get until this revision
        """

        if start is None:
            start = "base"
        elif start == "current":
            start = [r.revision for r in self.current()]
        else:
            start = getattr(start, "revision", start)

        if end is None:
            end = "heads"
        elif end == "current":
            end = [r.revision for r in self.current()]
        else:
            end = getattr(end, "revision", end)

        return list(self.script_directory.walk_revisions(start, end))

    def stamp(self, target="heads"):
        """Set the current database revision without running migrations.

        :param target: revision to set to, default 'heads'
        """

        target = "heads" if target is None else getattr(target, "revision", target)

        def do_stamp(revision, context):
            return self.script_directory._stamp_revs(target, revision)

        self.run_migrations(do_stamp)

    def upgrade(self, revision="heads"):
        """Run migrations to upgrade database.

        :param target: revision to go to, default 'heads'
        """

        revision = (
            "heads" if revision is None else getattr(revision, "revision", revision)
        )
        revision = str(revision)

        def do_upgrade(rev, context):
            return self.script_directory._upgrade_revs(revision, rev)

        self.run_migrations(do_upgrade)

    def downgrade(self, revision=-1):
        """Run migrations to downgrade database.

        :param target: revision to go down to, default -1
        """

        try:
            revision = int(revision)
        except ValueError:
            revision = getattr(revision, "revision", revision)
        else:
            if revision > 0:
                revision = -revision

        revision = str(revision)

        def do_downgrade(rev, context):
            return self.script_directory._downgrade_revs(revision, rev)

        self.run_migrations(do_downgrade)
