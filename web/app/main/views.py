from flask import render_template, request, abort, session, redirect, url_for
from . import main
from .use_cases import RetrieveCourseCatalog, RetrieveCourseCatalogCommand
from .use_cases import RetrieveCourseData, RetrieveCourseDataCommand
from .use_cases import PlaceAnInfoRequest, PlaceAnInfoRequestCommand
from .use_cases import RetrieveHomeRecommendations, RetrieveHomeRecommendationsCommand
from .use_cases import RetrieveCategories

users = {
    'user@test.com': {
        'password': '1234'
    }
}


@main.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        req = request.form

        user_email = req.get('email')
        password = req.get('password')

        user = users.get(user_email)

        if not user or password != user.get('password'):
            error = 'Invalid Credentials. Please try again.'
        else:
            session.clear()
            session['user_email'] = user_email

            return redirect(url_for('main.home'))

    return render_template('login.html', error=error)


@main.route('/logout')
def logout():
    session.pop('user_email', None)

    return redirect(url_for('main.home'))


@main.route('/', methods=['GET'])
def home():
    command = RetrieveHomeRecommendationsCommand(user_email=session.get('user_email'))
    response = RetrieveHomeRecommendations.execute(command)

    return render_template('home.html',
                           recommendations=response['recommendations'],
                           categories=response['categories'])


@main.route('/categories', methods=['GET'])
def categories():
    response = RetrieveCategories.execute()

    return render_template('categories.html', response=response)


@main.route('/catalog', methods=['GET'])
def catalog():
    command = RetrieveCourseCatalogCommand(page=request.args.get('page', default=1),
                                           sort_by=request.args.get('sort_by', default='leads'),
                                           category=request.args.get('category'))

    response = RetrieveCourseCatalog.execute(command)

    return render_template('course-catalog.html', response=response)


@main.route('/course/<int:course_id>', methods=['GET'])
def course(course_id):
    command = RetrieveCourseDataCommand(course_id, session.get('user_email'))

    response = RetrieveCourseData.execute(command)

    return render_template('course.html',
                           course=response['course'],
                           recommendations=response['recommendations'])


@main.route('/request-information', methods=['GET', 'POST'])
def request_information():
    if request.method == 'POST':
        email = request.form.get('email')
        course_id = request.form.get('courseId')

        command = PlaceAnInfoRequestCommand(course_id, email)
        response = PlaceAnInfoRequest.execute(command)

        if not response['success']:
            return abort(500)

    return render_template('request-information.html', response=response)
