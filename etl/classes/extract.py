from .db_service import DbService
import pandas as pd
from os import path


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

        if path.exists('leads_tmp.csv'):
            return pd.read_csv('leads_tmp.csv')

        df = self.create_df_from_query(query)
        df.to_csv('leads_tmp.csv', index=False)

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

        if path.exists('reviews_tmp.csv'):
            return pd.read_csv('reviews_tmp.csv')

        df = self.create_df_from_query(query)
        df.to_csv('reviews_tmp.csv', index=False)

        return df

