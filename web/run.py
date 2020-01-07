import os
from app import create_app

environment = os.environ.get('FLASK_ENV', 'development')

application = create_app('config.{}Config'.format(environment.capitalize()))

if __name__ == '__main__':
    application.run()
