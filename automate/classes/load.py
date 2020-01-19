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
