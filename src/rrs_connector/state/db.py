"""Database engine, schema initialization, and session factory helpers."""

from pathlib import Path

from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from rrs_connector.state.models import DbBase


def make_sqlite_url(db_path: Path) -> str:
    """Build a SQLAlchemy SQLite URL from a filesystem path."""

    return f"sqlite:///{db_path}"


def create_db_engine(db_path: Path) -> Engine:
    """Create a SQLAlchemy engine for the local SQLite state database."""

    db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(make_sqlite_url(db_path))


def initialize_database(engine: Engine) -> None:
    """Create all known state database tables if they do not exist.

    Columns added to a model later are added to an existing table here: new
    columns are always nullable, so old rows simply have them empty.
    """

    DbBase.metadata.create_all(engine)
    add_missing_columns(engine)


def add_missing_columns(engine: Engine) -> None:
    inspector = inspect(engine)
    for table in DbBase.metadata.sorted_tables:
        existing = {column["name"] for column in inspector.get_columns(table.name)}
        for column in table.columns:
            if column.name in existing:
                continue
            if not column.nullable:
                raise RuntimeError(
                    f"{table.name}.{column.name} is new and NOT NULL: "
                    "an existing database cannot get it without a migration"
                )
            column_type = column.type.compile(engine.dialect)
            with engine.begin() as connection:
                connection.execute(
                    text(
                        f'ALTER TABLE "{table.name}" '
                        f'ADD COLUMN "{column.name}" {column_type}'
                    )
                )


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create short-lived SQLAlchemy sessions bound to the given engine."""

    return sessionmaker(bind=engine, expire_on_commit=False)
