from flask import render_template, request, abort, session, redirect, url_for
from . import main
from .use_cases import RetrieveCourseCatalog, RetrieveCourseCatalogCommand
from .use_cases import RetrieveCourseData, RetrieveCourseDataCommand
from .use_cases import PlaceAnInfoRequest, PlaceAnInfoRequestCommand
from .use_cases import RetrieveHomeRecommendations, RetrieveHomeRecommendationsCommand
from .use_cases import RetrieveCategories

users = [
    '1460318498c1f53bb880ce2e6d9ef64b',
    '001d7fed950bc1f5834b2d4af75b1879',
    '0000b20311e912f825aadd5bd195d9d4',
    '0105ce929e7d4832f945e2b500eb86e0',
    '016602affb282ed547501f8a8b9283ed'
]

test_password = 'testing'


@main.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        req = request.form

        user = req.get('user')
        password = req.get('password')

        if user not in users or password != test_password:
            error = 'Invalid Credentials. Please try again.'
        else:
            session.clear()
            session['user_id'] = user
            session['user_name'] = 'User {}'.format(users.index(user) + 1)

            return redirect(url_for('main.home'))

    return render_template('login.html', users=users, error=error)


@main.route('/logout')
def logout():
    session.pop('user_id', None)
    session.pop('user_name', None)

    return redirect(url_for('main.home'))


@main.route('/', methods=['GET'])
def home():
    command = RetrieveHomeRecommendationsCommand(user_id=session.get('user_id'))
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
    command = RetrieveCourseDataCommand(course_id, session.get('user_id'))

    response = RetrieveCourseData.execute(command)

    return render_template('course.html',
                           course=response['course'],
                           recommendations=response['recommendations'])


@main.route('/request-information', methods=['POST'])
def request_information():
    email = request.form.get('email')
    course_id = request.form.get('courseId')

    command = PlaceAnInfoRequestCommand(course_id, email)
    response = PlaceAnInfoRequest.execute(command)

    if not response['success']:
        return abort(500)

    return render_template('request-information.html', response=response)
