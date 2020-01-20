#!/usr/bin/env python

import argparse

from utils import Output, is_valid_user, arguments
from classes import Model

parser = argparse.ArgumentParser(description='Performs a data modeling pipeline',
                                 usage='python model.py user password [OPTIONS]')

input_username, input_password, db_name, db_host = arguments(parser)

output = Output()


def model_data():
    model = Model(input_username, input_password, db_name, db_host)

    # Create course content similarity DataFrame
    output.write('Create a course content similarity DataFrame')
    output.warning('This process can take a long time')
    output.start_spinner('Creating a course content similarity DataFrame')

    try:
        model.create_course_content_similarity_df()
        output.spinner_success()
    except Exception as err:
        output.spinner_fail(str(err))
        exit(1)

    # Save courses content similarities to database
    output.write('Save courses content similarities to database')
    output.start_spinner('Saving courses content similarities to database')

    try:
        model.save_course_content_similarities()

        output.spinner_success()
    except Exception as err:
        output.spinner_fail(str(err))

    # Create leads user-item matrix
    output.write('Create leads user-item matrix')
    output.warning('This process can take a long time')
    output.start_spinner('Creating the leads user-item matrix')

    try:
        model.create_leads_user_item_matrix()
        output.spinner_success()

        # Compress leads user-item matrix
        output.write('Compress leads user-item matrix')
        output.start_spinner('Compressing the leads user-item matrix')

        model.compress_leads_user_item_matrix()

        output.spinner_success()

    except Exception as err:
        output.spinner_fail(str(err))
        exit(1)

    # Save user requested courses map to a file
    output.write('Save user requested courses map to a file')
    output.start_spinner('Saving user requested courses map to a file')

    try:
        model.save_user_courses_map('../web/data/user_requested_courses_map.pickle')

        output.spinner_success()
    except Exception as err:
        output.spinner_fail(str(err))

    # Create course-course recommendations DataFrame
    output.write('Create course-course recommendations DataFrame')
    output.warning('This process can take a long time')
    output.start_spinner('Creating the course-course recommendations DataFrame')

    try:
        model.create_course_course_recommendations_df()

        output.spinner_success()
    except Exception as err:
        output.spinner_fail(str(err))
        exit(1)

    # Save course-course recommendations to database
    output.write('Save course-course recommendations to database')
    output.start_spinner('Saving course-course recommendations to database')

    try:
        model.save_course_course_recommendations()

        output.spinner_success()
    except Exception as err:
        output.spinner_fail(str(err))


def main():
    output.title('START MODELING', color='magenta')
    output.start_spinner('Validating user credentials')
    if is_valid_user(input_username, input_password):
        output.spinner_success()
    else:
        output.spinner_fail('Invalid username or password')
        exit(1)

    model_data()

    output.success('Data modeling completed')


if __name__ == '__main__':
    main()
