from flask import Flask
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy

bootstrap = Bootstrap()
db = SQLAlchemy()


def create_app(config):
    app = Flask(__name__)

    with app.app_context():
        from .main import main as main_blueprint

        app.config.from_object(config)
        bootstrap.init_app(app)
        db.init_app(app)

        app.register_blueprint(main_blueprint)

    return app
