import logging
import sqlalchemy.engine
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


def make_connection_to_postgresql(host: str, port: str, user: str, password: str, db_name: str) -> sqlalchemy.engine.base.Engine or None:
    """Makes a connection to PostgreSQL DB."""
    try:
        postgres_str = f'postgresql://{user}:{password}@{host}:{port}/{db_name}'
        cnx = create_engine(postgres_str)
        cnx.connect()
    except OperationalError as e:
        logging.exception(e)
        cnx = None
    return cnx


