import pandas
import sqlalchemy.engine
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


def connection2db(host: str, port: str, user: str, password: str, db_name: str) -> sqlalchemy.engine.base.Engine:
    """Makes a connection to PostgreSQL DB."""
    try:
        postgres_str = f'postgresql://{user}:{password}@{host}:{port}/{db_name}'
        cnx = create_engine(postgres_str)
        cnx.connect()
        # print("Connection to {} successful.".format(db_name))
    except OperationalError as e:
        cnx = None
        print(e)
    return cnx


def put_df_into_remote_db(df: pandas.DataFrame,
                          table_name: str,
                          host: str,
                          port: str,
                          user: str,
                          password: str,
                          db_name: str,
                          if_exists: str) -> None:
    """Makes connection with remote using connection2db function and puts DataFrame (df) as SQL Table."""
    remote_cn = connection2db(host, port, user, password, db_name)
    try:
        df.to_sql(table_name, remote_cn, index=False, if_exists=if_exists)
        print('DataFrame successfully inserted as Table to PostgreSQL DB.')
    except ValueError as e:
        print('Cannot put table to remote.')
        print(e)


def get_from_remote_db(table_name_or_query: str,
                       host: str,
                       port: str,
                       user: str,
                       password: str,
                       db_name: str) -> pandas.DataFrame:
    """Makes connection with remote using connection2db function and gets data from SQL Table.
    table_name_or_query parameter can represent table name or valid SQL Query."""
    remote_cn = connection2db(host, port, user, password, db_name)
    try:
        df = pandas.read_sql(table_name_or_query, remote_cn)
        return df
    except sqlalchemy.exc.ProgrammingError as e:
        print('Cannot get table or query.')
        print(e)


