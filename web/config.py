
class Config:
    DEBUG = False
    TESTING = False
    SECRET_KEY = 'OCML3BRawWEUeaxcuKHLpw'
    DB_USER = 'bc0e0e4f733dda'
    DB_PASSWORD = '00f61efe'
    DB_NAME = 'heroku_8149febc614deb5'
    DB_HOST = 'eu-cdbr-west-02.cleardb.net'
    SQLALCHEMY_DATABASE_URI = 'mysql://{}:{}@{}/{}'.format(DB_USER,
                                                           DB_PASSWORD,
                                                           DB_HOST,
                                                           DB_NAME)
    SQLALCHEMY_TRACK_MODIFICATIONS = False


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
