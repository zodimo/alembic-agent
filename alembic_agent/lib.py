import logging

logger = logging.getLogger(__name__)
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from alembic.config import Config
from alembic import util
from sqlalchemy import engine_from_config, create_engine, MetaData
from sqlalchemy.engine import Engine
import os, sys, io
from contextlib import contextmanager


class AlembicAgent:
    """
    Only (Online) upgrade, downgrade and stamp use env.py
    Offline removed to return the sql instead of stdout

    Commands not implemented, use alembic cli:
        list_templates
        init
        revision
        merge
        edit
    """

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
        # script_location is where env.py lives
        script_location = os.path.join(current_dir_path, "script")
        # version_locations are where the migrations live
        version_location = os.path.join(current_dir_path, "migrations")
        version_locations = [version_location]
        self.__config.set_main_option("script_location", script_location)
        self.__config.set_main_option(
            "version_locations", ",".join(version_locations)
        )
        return self.__config

    @property
    def script_directory(self) -> ScriptDirectory:
        if isinstance(self.__script_directory, ScriptDirectory):
            return self.__script_directory
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

    def upgrade(self, revision, sql=False, tag=None):
        """Upgrade to a later version.

        :param revision: string revision target or range for --sql mode

        :param sql: if True, use ``--sql`` mode

        :param tag: an arbitrary "tag" that can be intercepted by custom
        ``env.py`` scripts via the :meth:`.EnvironmentContext.get_tag_argument`
        method.

        """
        config = self.config
        script = self.script_directory
        config.attributes["engine"] = self.engine
        config.attributes["target_metadata"] = self.target_metadata

        output_buffer = io.StringIO()
        config.attributes["output_buffer"] = output_buffer

        starting_rev = None
        if ":" in revision:
            if not sql:
                raise util.CommandError("Range revision not allowed")
            starting_rev, revision = revision.split(":", 2)

        def do_upgrade(rev, context):
            return script._upgrade_revs(revision, rev)

        with EnvironmentContext(
            config,
            script,
            fn=do_upgrade,
            as_sql=sql,
            starting_rev=starting_rev,
            destination_rev=revision,
            tag=tag,
        ):
            script.run_env()
            output_buffer.seek(0)
            return output_buffer.read()

    def downgrade(self, revision, sql=False, tag=None):
        """Revert to a previous version.

        :param revision: string revision target or range for --sql mode

        :param sql: if True, use ``--sql`` mode

        :param tag: an arbitrary "tag" that can be intercepted by custom
        ``env.py`` scripts via the :meth:`.EnvironmentContext.get_tag_argument`
        method.

        """

        config = self.config
        script = self.script_directory
        config.attributes["engine"] = self.engine

        output_buffer = io.StringIO()
        config.attributes["output_buffer"] = output_buffer

        starting_rev = None
        if ":" in revision:
            if not sql:
                raise util.CommandError("Range revision not allowed")
            starting_rev, revision = revision.split(":", 2)
        elif sql:
            raise util.CommandError(
                "downgrade with --sql requires <fromrev>:<torev>"
            )

        def do_downgrade(rev, context):
            return script._downgrade_revs(revision, rev)

        with EnvironmentContext(
            config,
            script,
            fn=do_downgrade,
            as_sql=sql,
            starting_rev=starting_rev,
            destination_rev=revision,
            tag=tag,
        ):
            script.run_env()
            output_buffer.seek(0)
            return output_buffer.read()

    def show(self, revision):
        """Show the revision(s) denoted by the given symbol.
       
        :param revision: string revision target

        """
        script = self.script_directory
        return script.get_revisions(revision)

    def history(self, rev_range="base:heads", indicate_current=False):
        """List changeset scripts in chronological order.

        :param rev_range: string revision range

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
                    sc._db_current_indicator = sc in currents
                history.insert(0, sc)

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

    def heads(self, resolve_dependencies=False):
        """Show current available heads in the script directory.

        :param resolve_dependencies: treat dependency version as down revisions.
        """

        if resolve_dependencies:
            return self.script_directory.get_revisions("heads")

        return self.script_directory.get_revisions(
            self.script_directory.get_heads()
        )

    @property
    def branches(self):
        """Show current branch points."""
        script = self.script_directory
        branches = list()
        for sc in script.walk_revisions():
            if sc.is_branch_point:
                branches.append(sc)
        return branches

    @property
    def current(self):
        """Display the current revision for a database."""
        with self.migration_context as migration_context:
            return self.script_directory.get_revisions(
                migration_context.get_current_heads()
            )

    def stamp(self, revision, sql=False, tag=None, purge=False):
        """'stamp' the revision table with the given revision; don't
        run any migrations.

        :param revision: target revision or list of revisions.   May be a list
        to indicate stamping of multiple branch heads.

        .. note:: this parameter is called "revisions" in the command line
            interface.

        .. versionchanged:: 1.2  The revision may be a single revision or
            list of revisions when stamping multiple branch heads.

        :param sql: use ``--sql`` mode

        :param tag: an arbitrary "tag" that can be intercepted by custom
        ``env.py`` scripts via the :class:`.EnvironmentContext.get_tag_argument`
        method.

        :param purge: delete all entries in the version table before stamping.

        .. versionadded:: 1.2

        """

        config = self.config
        script = self.script_directory
        config.attributes["engine"] = self.engine

        output_buffer = io.StringIO()
        config.attributes["output_buffer"] = output_buffer

        starting_rev = None
        if sql:
            destination_revs = []
            for _revision in util.to_list(revision):
                if ":" in _revision:
                    srev, _revision = _revision.split(":", 2)

                    if starting_rev != srev:
                        if starting_rev is None:
                            starting_rev = srev
                        else:
                            raise util.CommandError(
                                "Stamp operation with --sql only supports a "
                                "single starting revision at a time"
                            )
                destination_revs.append(_revision)
        else:
            destination_revs = util.to_list(revision)

        def do_stamp(rev, context):
            return script._stamp_revs(util.to_tuple(destination_revs), rev)

        with EnvironmentContext(
            config,
            script,
            fn=do_stamp,
            as_sql=sql,
            starting_rev=starting_rev if sql else None,
            destination_rev=util.to_tuple(destination_revs),
            tag=tag,
            purge=purge,
        ):
            script.run_env()
            output_buffer.seek(0)
            return output_buffer.read()
