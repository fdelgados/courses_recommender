from .db_service import DbService
import pandas as pd


class Extract(DbService):
    def extract_leads(self) -> pd.DataFrame:
        """Create a leads dataframe from query

        :return: a dataframe with leads
        """
        query = '''SELECT user_id,
            course_id,
            course_title,
            course_description,
            course_category,
            center,
            created_on
        FROM leads
        ORDER BY created_on DESC
        '''

        df = self.create_df_from_query(query)

        return df

    def extract_reviews(self) -> pd.DataFrame:
        """Create a reviews dataframe from query

        :return: a dataframe with reviews
        """
        query = '''SELECT user_id,
            course_id,
            course_title,
            course_category,
            course_description,
            center,
            rating,
            created_on
        FROM reviews
        ORDER BY created_on DESC
        '''

        df = self.create_df_from_query(query)

        return df

