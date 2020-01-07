from flask import Flask
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy

bootstrap = Bootstrap()
db = SQLAlchemy()


def create_app(config):
    app = Flask(__name__)

    app.config.from_object(config)
    bootstrap.init_app(app)
    db.init_app(app)

    from . import main
    app.register_blueprint(main.main)

    return app
