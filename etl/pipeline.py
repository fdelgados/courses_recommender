#!/usr/bin/env python

import argparse

from utils import Output
from classes import Extract, Transform

parser = argparse.ArgumentParser(description='Performs an ETL pipeline',
                                 usage='python pipeline.py user password [OPTIONS]')

parser.add_argument('username', help='Username')
parser.add_argument('password', help='Password')

parser.add_argument('-n', '--db-name',
                    dest='db_name',
                    help='Database name',
                    metavar='')

parser.add_argument('-s', '--db-server',
                    dest='db_host',
                    help='Database host',
                    metavar='')

args = parser.parse_args()

input_username = args.username
input_password = args.password
db_name = args.db_name
db_host = args.db_host


def is_valid_user(username, password):
    with open('./users', 'r') as file:
        for credentials in file:
            usr, pwd = credentials.split()
            if username == usr and password == pwd:
                return True

    return False


def main():
    output = Output()

    output.title('START ETL PIPELINE')
    output.start_spinner('Validating user credentials')
    if is_valid_user(input_username, input_password):
        output.spinner_success()
    else:
        output.spinner_fail('Invalid username or password')
        exit(1)

    # Data extraction
    extract = Extract(input_username, input_password, db_name, db_host)

    output.title('DATA EXTRACTION')
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

    output.title('DATA TRANSFORMATION')

    # Remove duplicated rows
    output.write('Removing duplicated data')

    transform = Transform(leads_df, reviews_df)

    columns = ['user_id', 'course_id']

    output.start_spinner('Removing duplicated leads')
    duplicated_leads_removed = transform.remove_duplicated_leads(columns)

    output.spinner_success('Removed {} duplicated leads'.format(duplicated_leads_removed))

    output.start_spinner('Removing duplicated reviews')
    duplicated_reviews_removed = transform.remove_duplicated_reviews(columns)

    output.spinner_success('Removed {} duplicated reviews'.format(duplicated_reviews_removed))


if __name__ == '__main__':
    main()
