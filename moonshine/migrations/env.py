from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config


# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    engine = config.attributes.get("engine", None)
    target_metadata = config.attributes.get("target_metadata", None)

    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    raise Exception("How did you end up here ?")
else:
    run_migrations_online()
