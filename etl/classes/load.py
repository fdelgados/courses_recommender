import pickle
from typing import Dict
from .db_service import DbService
import pandas as pd


class Load(DbService):
    def save_courses(self, courses_df: pd.DataFrame):
        """Saves courses DataFrame to database

        :param courses_df: Courses DataFrame
        """
        connection = self.connection()

        sql_drop = 'DROP TABLE IF EXISTS `courses`'
        connection.execute(sql_drop)

        sql_create = """CREATE TABLE `courses` (
          `id` varchar(9) NOT NULL,
          `title` text,
          `description` text,
          `center` varchar(100) NOT NULL,
          `avg_rating` double DEFAULT NULL,
          `num_reviews` int(11) DEFAULT NULL,
          `weighted_rating` double DEFAULT NULL,
          `number_of_leads` int(11) DEFAULT NULL,
          `category_id` int(11) NOT NULL,
          PRIMARY KEY (`id`),
          KEY `courses_category_id_index` (`category_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        connection.execute(sql_create)

        courses_df.to_sql('courses', con=connection, if_exists='append', index=False)

    def save_leads(self, leads_df: pd.DataFrame):
        """Saves leads DataFrame to database

        :param leads_df: Leads DataFrame
        """
        connection = self.connection()
        leads_df = leads_df[['user_id', 'course_id', 'created_on']]

        sql_drop = 'DROP TABLE IF EXISTS `clean_leads`'
        connection.execute(sql_drop)

        sql_create = """CREATE TABLE `clean_leads` (
            `user_id`    CHAR(36) NOT NULL,
            `course_id`  VARCHAR(12) NOT NULL,
            `created_on` DATETIME NOT NULL,
            PRIMARY KEY (`user_id`, `course_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        connection.execute(sql_create)

        leads_df.to_sql('clean_leads', con=connection, if_exists='append', index=False)

    def save_reviews(self, reviews_df: pd.DataFrame):
        """Saves reviews DataFrame to database

        :param reviews_df: Reviews DataFrame
        """
        connection = self.connection()
        reviews_df = reviews_df[['user_id', 'course_id', 'rating', 'created_on']]

        sql_drop = 'DROP TABLE IF EXISTS `clean_reviews`'
        connection.execute(sql_drop)

        sql_create = """CREATE TABLE `clean_reviews` (
            `user_id`    CHAR(36) NOT NULL,
            `course_id`  VARCHAR(12) NOT NULL,
            `rating`     INT NOT NULL,
            `created_on` DATETIME NOT NULL,
            PRIMARY KEY (`user_id`, `course_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        connection.execute(sql_create)

        reviews_df.to_sql('clean_reviews', con=connection, if_exists='append', index=False)

    def save_categories(self, categories_df: pd.DataFrame):
        """Saves categories DataFrame to database

        :param categories_df: Categories DataFrame
        """
        connection = self.connection()

        sql_drop = 'DROP TABLE IF EXISTS `categories`'
        connection.execute(sql_drop)

        sql_create = """CREATE TABLE `categories` (
          `id` int(11) NOT NULL,
          `name` varchar(200) NOT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """

        connection.execute(sql_create)

        categories_df.to_sql('categories', con=connection, if_exists='append', index=False)

    def save_course_content_similarities(self, course_content_similarities_df: pd.DataFrame):
        """Saves courses content similarities DataFrame to database

        :param course_content_similarities_df: Courses content similarities DataFrame
        """
        return

        connection = self.connection()

        sql_drop = 'DROP TABLE IF EXISTS `courses_similarities`'
        connection.execute(sql_drop)

        sql_create = """CREATE TABLE `courses_similarities` (
          `a_course_id` varchar(9) NOT NULL,
          `another_course_id` varchar(9) NOT NULL,
          `similarity` double NOT NULL,
          PRIMARY KEY (`a_course_id`, `another_course_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        connection.execute(sql_create)

        # Save course similarities to database
        course_content_similarities_df.to_sql('courses_similarities', con=connection, if_exists='append', index=False)

    def save_course_course_recommendations(self, course_course_recs_df: pd.DataFrame):
        """Saves courses recommendations DataFrame to database

        :param course_course_recs_df: Courses recommendations DataFrame
        """
        conn = self.connection()

        sql_drop = 'DROP TABLE IF EXISTS `recommended_courses_by_leads`'
        conn.execute(sql_drop)

        sql_create = """CREATE TABLE `recommended_courses_by_leads` (
          `course` varchar(9) NOT NULL,
          `recommended` varchar(9) NOT NULL,
          PRIMARY KEY (`course`, `recommended`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        conn.execute(sql_create)

        # Save recommendations to database
        course_course_recs_df.to_sql('recommended_courses_by_leads', con=conn, if_exists='append', index=False)

    def save_user_courses_map(self, user_courses_map: Dict, file_name: str):
        with open(file_name, 'wb') as file:
            pickle.dump(user_courses_map, file)

    def save_users_distances(self, distances_df: pd.DataFrame):
        """Saves users distances DataFrame to database

        :param distances_df: Users distances DataFrame
        """
        conn = self.connection()

        sql_drop = 'DROP TABLE IF EXISTS `users_distances`'
        conn.execute(sql_drop)

        sql_create = """CREATE TABLE `users_distances` (
          `a_user_id` char(36) NOT NULL,
          `another_user_id` char(36) NOT NULL,
          `eucl_distance` double NOT NULL,
          PRIMARY KEY (`a_user_id`, `another_user_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        conn.execute(sql_create)

        # Save distances to database
        distances_df.to_sql('users_distances', con=conn, if_exists='append', index=False)

