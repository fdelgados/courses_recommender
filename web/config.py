import os
from pathlib import Path

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    DEBUG = False
    TESTING = False
    SECRET_KEY = 'nqkUR,+j>3#D7n#('
    DB_USER = 'bc0e0e4f733dda'
    DB_PASSWORD = '00f61efe'
    DB_NAME = 'heroku_8149febc614deb5'
    DB_HOST = 'eu-cdbr-west-02.cleardb.net'
    SQLALCHEMY_DATABASE_URI = 'mysql://{}:{}@{}/{}'.format(DB_USER,
                                                           DB_PASSWORD,
                                                           DB_HOST,
                                                           DB_NAME)
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    DATA_DIR = os.path.join(Path(basedir).parent, 'data')
    APPLICATION_ROOT = basedir


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    TESTING = True


class ProductionConfig(Config):
    DB_USER = 'bc0e0e4f733dda'
    DB_PASSWORD = '00f61efe'
    DB_NAME = 'heroku_8149febc614deb5'
    DB_HOST = 'eu-cdbr-west-02.cleardb.net'
    SQLALCHEMY_DATABASE_URI = 'mysql://{}:{}@{}/{}'.format(DB_USER,
                                                           DB_PASSWORD,
                                                           DB_HOST,
                                                           DB_NAME)
