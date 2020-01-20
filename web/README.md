# Course recommender web
This is the code of the demo web application of the course recommendation system project. The application is accessible [here](https://courses-recommender.herokuapp.com/)
and has been built using [Flask](https://flask.palletsprojects.com/en/1.1.x/).

## Table of contents

* [Project site](#project_site)
* [Instructions](#instructions)
* [Code structure](#code_structure)

<a id="project_site"></a>
## Project site
This demo web application has a project page and can be visited at [https://fdelgados.github.io/recommendations-web/](https://fdelgados.github.io/recommendations-web/)

<a id="instructions"></a>
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

    ![Home page](https://github.com/fdelgados/recommendations-web/blob/master/docs/img/home_screenshot.png)
    
7. To stop the application, press `CTRL+C`

<a id="code_structure"></a>
## Code structure

The code is structured following the layered architecture:

1. Presentation layer
2. Application layer
3. Business logic layer
4. Persistence layer

### Presentation layer

In this layer are the html files, they are the views with which the user will interact. This files are in `app/templates`.
The file `app/main/views.py` is the router file, it is responsible for processing user requests and returns the corresponding response.

### Application layer

This layer is composed of a single module, `app/main/use_cases.py`. There is a class for each user request or use case. These classes receive data from the presentation layer wrapped in classes called commands.
Each use case makes requests to the persistence layer directly or through the business logic layer and retrieves the necessary data in each use case.

### Business logic layer

In the `app/recommender.py` module there is the business logic of the application. The `Recommender` class has the code necessary to make the three types of recommendations used in the web.

### Persistence layer

The persistence layer is responsible for managing data and communicates with the persistence system, there are two type of classes: model classes that represents entities of our application
(Course, Lead and Category), and repositories that are responsible for performing the queries to the database and build and return collections of models.
