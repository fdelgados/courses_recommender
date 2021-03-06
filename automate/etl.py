#!/usr/bin/env python

import argparse
from os import path
from typing import Tuple

import pandas as pd

from utils import Output, is_valid_user, arguments
from classes import Extract, Transform, Load

parser = argparse.ArgumentParser(description='Performs an ETL pipeline',
                                 usage='python etl.py user password [OPTIONS]')

input_username, input_password, db_name, db_host = arguments(parser)

output = Output()


def tmp_files() -> bool:
    """Checks if temporary data file exist

    :return: Returns `True` if temporary data exists, returns `False` otherwise
    """
    return path.exists('.tmp/reviews.csv') and \
        path.exists('.tmp/courses.csv') and \
        path.exists('.tmp/leads.csv') and \
        path.exists('.tmp/categories.csv')


def extract_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extracts leads and reviews data from database

    :return: A tuple with the leads DataFrame and reviews DataFrame
    """
    extract = Extract(input_username, input_password, db_name, db_host)

    try:
        output.start_spinner('Extracting leads from database')
        leads_df = extract.extract_leads()

        output.spinner_success('Leads extraction complete: {} leads'.format(leads_df.shape[0]))

        output.start_spinner('Extracting reviews from database')
        reviews_df = extract.extract_reviews()

        output.spinner_success('Reviews extraction complete: {} reviews'.format(reviews_df.shape[0]))
    except SystemError as err:
        output.spinner_fail(str(err))
        exit(1)

    return leads_df, reviews_df


def transform_data(leads_df: pd.DataFrame, reviews_df: pd.DataFrame) -> Transform:
    """Performs data transformation

    :param leads_df: Leads DataFrame
    :param reviews_df: Reviews DataFrame
    :return: Transform object
    """
    transform = Transform(leads_df, reviews_df)

    # Remove duplicated rows
    output.write('Removing duplicated data')

    try:
        columns = ['user_id', 'course_id']

        output.start_spinner('Removing duplicated leads')
        duplicated_leads_removed = transform.remove_duplicated_leads(columns)

        output.spinner_success('Removed {} duplicated leads'.format(duplicated_leads_removed))

        output.start_spinner('Removing duplicated reviews')
        duplicated_reviews_removed = transform.remove_duplicated_reviews(columns)

        output.spinner_success('Removed {} duplicated reviews'.format(duplicated_reviews_removed))
    except ValueError as err:
        output.spinner_fail(str(err))
        exit(1)

    # Replace missing values
    output.write('Replacing missing values')

    try:
        output.start_spinner('Replacing missing values in leads DataFrame')
        leads_values_replaced = transform.replace_leads_null_values('course_description', '')

        output.spinner_success('Replaced {} missing descriptions in leads DataFrame'.format(leads_values_replaced))

        output.start_spinner('Replacing missing values in reviews DataFrame')
        reviews_values_replaced = transform.replace_reviews_null_values('course_description', '')

        output.spinner_success('Replaced {} missing descriptions in reviews DataFrame'.format(reviews_values_replaced))
    except ValueError as err:
        output.spinner_fail(str(err))
        exit(1)

    # Change data types
    output.write('Changing data types')
    output.start_spinner('Changing ids to string type')
    try:
        transform.change_column_type('course_id', str)
        output.spinner_success()
    except ValueError as err:
        output.spinner_fail(str(err))
        exit(1)

    # Create a courses DataFrame
    output.write('Create a courses DataFrame')
    output.start_spinner('Creating the courses DataFrame')

    try:
        courses_df = transform.create_courses_df()

        output.spinner_success('Courses DataFrame created with {} rows'.format(courses_df.shape[0]))
    except Exception as err:
        output.spinner_fail(str(err))
        exit(1)

    # Detect courses language
    output.write('Detect courses language')
    output.warning('This process can take a long time')
    output.start_spinner('Detecting courses language')
    try:
        courses_df = transform.detect_courses_language()
        output.spinner_success('Language detection complete. {} courses are not written in English'. format(
            courses_df[courses_df['lang'] != 'en'].shape[0]))
    except Exception as err:
        output.spinner_fail(str(err))
        exit(1)

    # Remove courses that are not in English
    output.write('Remove courses that are not in English')
    output.start_spinner('Removing courses that are not in English')
    try:
        transform.remove_not_in_english_courses()
        output.spinner_success()
    except Exception as err:
        output.spinner_fail(str(err))
        exit(1)

    # Add data to courses DataFrame
    output.write('Add data to courses DataFrame')
    output.start_spinner('Adding data to courses DataFrame')
    try:
        transform.add_data_to_courses(25)

        output.spinner_success()
    except Exception as err:
        output.spinner_fail(str(err))
        exit(1)

    # Create a categories DataFrame
    output.write('Create a categories DataFrame')
    output.start_spinner('Creating a categories DataFrame')

    categories_df = transform.create_categories_df()

    output.spinner_success('Categories DataFrame creation complete. There are {} categories'.format(
        categories_df.shape[0]))

    return transform


def load_data(transform: Transform) -> bool:
    """Load clean data to database

    :param transform: Transform class, containing all DataFrames to be loaded
    :return: Returns `True` if loading process has been completed successfully, returns `False` otherwise
    """
    load = Load(input_username, input_password, db_name, db_host)
    load_errors = 0

    # Save courses to database
    output.write('Save courses to database')
    output.start_spinner('Saving courses to database')

    try:
        load.save_courses(transform.courses_df)

        output.spinner_success()
    except Exception as err:
        output.spinner_fail(str(err))
        load_errors += 1

    # Save leads to database
    output.write('Save leads to database')
    output.start_spinner('Saving leads to database')

    try:
        load.save_leads(transform.leads_df)

        output.spinner_success()
    except Exception as err:
        output.spinner_fail(str(err))
        load_errors += 1

    # Save reviews to database
    output.write('Save reviews to database')
    output.start_spinner('Saving reviews to database')

    try:
        load.save_reviews(transform.reviews_df)

        output.spinner_success()
    except Exception as err:
        output.spinner_fail(str(err))
        load_errors += 1

    # Save categories to database
    output.write('Save categories to database')
    output.start_spinner('Saving categories to database')

    try:
        load.save_categories(transform.categories_df)

        output.spinner_success()
    except Exception as err:
        output.spinner_fail(str(err))
        load_errors += 1

    return load_errors == 0


def main():
    output.title('START ETL PIPELINE', color='magenta')
    output.start_spinner('Validating user credentials')
    if is_valid_user(input_username, input_password):
        output.spinner_success()
    else:
        output.spinner_fail('Invalid username or password')
        exit(1)

    output.title('DATA EXTRACTION')

    if tmp_files():
        # Extract from tmp files
        output.info('Extracting data from temporary files')

        leads_df = pd.read_csv('.tmp/leads.csv')
        reviews_df = pd.read_csv('.tmp/reviews.csv')

        transform = Transform(leads_df, reviews_df)
        transform.courses_df = pd.read_csv('.tmp/courses.csv')
        transform.categories_df = pd.read_csv('.tmp/categories.csv')

    else:
        # Data extraction
        leads_df, reviews_df = extract_data()

        # Data transformation
        output.title('DATA TRANSFORMATION')

        transform = transform_data(leads_df, reviews_df)

    # Data load
    output.title('DATA LOAD')

    # If there is an error in data loading, dataframes will be saved into a local csv files
    # in this way, when executing the process again, the extracted data will already be clean
    if load_data(transform):
        output.success('ETL pipeline completed')
    else:
        transform.reviews_df.to_csv('.tmp/reviews.csv', index=False)
        transform.leads_df.to_csv('.tmp/leads.csv', index=False)
        transform.courses_df.to_csv('.tmp/courses.csv', index=False)
        transform.categories_df.to_csv('.tmp/categories.csv', index=False)

        output.warning('ETL pipeline completed with errors')


if __name__ == '__main__':
    main()
