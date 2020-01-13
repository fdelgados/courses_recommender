# Course recommender web
This is the code of the demo web application of the course recommendation system project. The application is accessible [here](https://courses-recommender.herokuapp.com/)
and has been built using [Flask](https://flask.palletsprojects.com/en/1.1.x/)

## Instructions
To run this project on a local machine, you have to follow these steps:

1. Make sure you have python 3.5 or higher installed.
2. Clone the github project in your local machine:

     `$ git clone git@github.com:fdelgados/recommendations-web.git`
3. Enter the 'recommendations-web' directory:

    `$ cd recommendations-web`
4. Install the project dependencies:

    `$ pip install -r requirements.txt`
5. Run the Flask application:

    `$ export FLASK_APP=run.py && export FLASK_ENV=development && python -m flask run`
    
    In the console, you should see something similar to this:
    ```shell
    * Serving Flask app "run.py" (lazy loading)
    * Environment: development
    * Debug mode: on
    * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
    * Restarting with stat
    * Debugger is active!
    * Debugger PIN: 148-090-871
    ```
6. Now, open your browser and go to http://localhost:5000
    
    
