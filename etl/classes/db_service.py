from sqlalchemy import create_engine, exc
import pandas as pd


class DbService:
    DEFAULT_DB_NAME = 'heroku_8149febc614deb5'
    DEFAULT_DB_HOST = 'eu-cdbr-west-02.cleardb.net'

    def __init__(self, db_user: str, db_password: str, db_name: str = None, db_host: str = None):
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name if db_name else self.DEFAULT_DB_NAME
        self.db_host = db_host if db_host else self.DEFAULT_DB_HOST

    def create_df_from_query(self, query: str) -> pd.DataFrame:
        """Runs the `query` in the database and build a `pandas.DataFrame` from the response

        :param query: A select query
        :return: A `pandas.DataFrame`
        """
        try:
            return pd.read_sql_query(query, con=self.connection())
        except exc.OperationalError:
            raise SystemError('Cannot connect to the database')

    def connection(self):
        """ Creates a new connection to database

        :return: database engine
        """

        return create_engine('mysql+pymysql://{}:{}@{}/{}'.format(self.db_user,
                                                                  self.db_password,
                                                                  self.db_host,
                                                                  self.db_name))
