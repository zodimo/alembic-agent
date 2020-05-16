import logging

logger = logging.getLogger(__name__)
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from alembic.config import Config
from alembic import util
from sqlalchemy import engine_from_config, create_engine, MetaData
from sqlalchemy.engine import Engine
import os, sys
from contextlib import contextmanager


class Moonshine:
    __config = None
    __engine = None
    __script_directory = None
    __environment_context = None

    target_metadata = MetaData(
        naming_convention={
            "ix": "ix_%(column_0_N_name)s",
            "uq": "uq_%(table_name)s_%(column_0_N_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )

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
        # set defaults for script_location and version_locations
        current_dir_path = os.path.dirname(os.path.realpath(__file__))
        script_location = os.path.join(current_dir_path, "migrations")
        version_locations = [script_location]
        self.__config.set_main_option("script_location", script_location)
        self.__config.set_main_option(
            "version_locations", ",".join(version_locations)
        )
        return self.__config

    @property
    def script_directory(self) -> ScriptDirectory:
        if isinstance(self.__script_directory, ScriptDirectory):
            return self.__script_directory
        logger.debug(
            f"config script_location:{self.config.get_main_option('script_location')}"
        )
        self.__script_directory = ScriptDirectory.from_config(self.config)
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
        self.__environment_context = EnvironmentContext(
            self.config, self.script_directory
        )
        return self.__environment_context

    @property
    @contextmanager
    def migration_context(self):
        with self.engine.connect() as conn:
            env = self.environment_context
            env.configure(connection=conn)
            yield env.get_context()

    def run_migrations(self, fn, **kwargs):
        """Configure an Alembic :class:`~alembic.runtime.migration.MigrationContext` to run migrations for the given function.

        This takes the place of Alembic's env.py file, specifically the ``run_migrations_online`` function.

        :param fn: use this function to control what migrations are run
        :param kwargs: extra arguments passed to ``upgrade`` or ``downgrade`` in each revision
        """

        env = self.environment_context

        with self.engine.connect() as connection:
            env.configure(
                connection=connection,
                fn=fn,
                target_metadata=self.target_metadata,
            )

            with env.begin_transaction():
                env.run_migrations(**kwargs)

    @property
    def current(self):
        """Get the list of current revisions."""
        with self.migration_context as migration_context:
            return self.script_directory.get_revisions(
                migration_context.get_current_heads()
            )

    def heads(self, resolve_dependencies=False):
        """Get the list of revisions that have no child revisions.

        :param resolve_dependencies: treat dependencies as down revisions
        """

        if resolve_dependencies:
            return self.script_directory.get_revisions("heads")

        return self.script_directory.get_revisions(
            self.script_directory.get_heads()
        )

    def branches(self):
        """Get the list of revisions that have more than one next revision."""

        return [
            revision
            for revision in self.script_directory.walk_revisions()
            if revision.is_branch_point
        ]

    def history(
        self, rev_range="base:heads", verbose=False, indicate_current=False
    ):
        """List changeset scripts in chronological order.

        :param config: a :class:`.Config` instance.

        :param rev_range: string revision range

        :param verbose: output in verbose mode.

        :param indicate_current: indicate current revision.

        ..versionadded:: 0.9.9

        """

        if rev_range is not None:
            if ":" not in rev_range:
                raise util.CommandError(
                    "History range requires [start]:[end], "
                    "[start]:, or :[end]"
                )
            base, head = rev_range.strip().split(":")
        else:
            base = head = None

        environment = (
            util.asbool(self.config.get_main_option("revision_environment"))
            or indicate_current
        )

        def _display_history(base, head, currents=()):

            history = list()
            for sc in self.script_directory.walk_revisions(
                base=base or "base", head=head or "heads"
            ):
                if indicate_current:
                    sc._db_current_indicator = sc.revision in currents
                history.append(sc)

            return history

        def _display_history_w_current(base, head):
            def _display_current_history(rev):
                if head == "current":
                    return _display_history(base, rev, rev)
                elif base == "current":
                    return _display_history(rev, head, rev)
                else:
                    return _display_history(base, head, rev)
                return []

            rev = self.current
            return _display_current_history(rev)

        if base == "current" or head == "current" or environment:
            return _display_history_w_current(base, head)
        else:
            return _display_history(base, head)

    def stamp(self, target="heads"):
        """Set the current database revision without running migrations.

        :param target: revision to set to, default 'heads'
        """

        target = (
            "heads" if target is None else getattr(target, "revision", target)
        )

        def do_stamp(revision, context):
            return self.script_directory._stamp_revs(target, revision)

        self.run_migrations(do_stamp)

    def upgrade(self, revision="heads"):
        """Run migrations to upgrade database.

        :param target: revision to go to, default 'heads'
        """

        revision = (
            "heads"
            if revision is None
            else getattr(revision, "revision", revision)
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
