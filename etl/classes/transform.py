from typing import List, Union

import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
from scipy.sparse import csr_matrix

from .rating_functions import all_avg_rating, average_rating, weighted_rating, num_reviews
from txtools.normalizer import clean_text, TextNormalizer
from txtools.similarity import Similarity
from txtools.utils import LangDetector


class Transform:
    def __init__(self, leads_df: pd.DataFrame, reviews_df: pd.DataFrame):
        self.leads_df = leads_df
        self.reviews_df = reviews_df
        self.courses_df = None
        self.categories_df = None
        self.leads_user_item_matrix = None
        self.ratings_user_item_matrix = None
        self.user_requested_courses_map = {}
        self.courses_content_sims_df = None
        self.course_course_recs_df = None

    def remove_duplicated_leads(self, subset: Union[List[str], str] = None) -> int:
        """Removes duplicated rows from leads DataFrame

        :param subset: Column name/s to identify duplicates. If it is `None` all columns will be used
        :return: Number of rows removed
        """
        num_rows_before = self.leads_df.shape[0]
        self.leads_df = Transform.__remove_duplicates__(self.leads_df, subset=subset)

        return num_rows_before - self.leads_df.shape[0]

    def remove_duplicated_reviews(self, subset: Union[List[str], str] = None) -> int:
        """Removes duplicated rows from reviews DataFrame

        :param subset: Column name/s to identify duplicates. If it is `None` all columns will be used
        :return: Number of rows removed
        """
        num_rows_before = self.reviews_df.shape[0]

        self.reviews_df = Transform.__remove_duplicates__(self.reviews_df, subset=subset)

        return num_rows_before - self.reviews_df.shape[0]

    @staticmethod
    def __remove_duplicates__(df: pd.DataFrame, subset: Union[List[str], str] = None) -> pd.DataFrame:
        """Removes duplicated rows from `df`

        :param df: A pandas DataFrame
        :param subset: Column name/s to identify duplicates. If it is `None` all columns will be used
        :return: The DataFrame with no duplicated rows
        """
        if subset is None:
            subset = df.columns
        else:
            Transform.__guard_against_non_existent_columns__(df, subset)

        if df.duplicated(subset).sum() > 0:
            df.drop_duplicates(subset, inplace=True, keep='last')

        return df

    def replace_reviews_null_values(self, column: str, replacement: Union[str, int, float]) -> int:
        """Fills null values in `column` with `replacement` in reviews DataFrame

        :param column: Column name/s to search for null values
        :param replacement: Value to use to replace nulls
        :return: The number of values that has been replaced
        """
        num_nulls = self.reviews_df[column].isnull().sum()

        self.reviews_df = Transform.__replace_nulls__(self.reviews_df, column, replacement)

        return num_nulls

    def replace_leads_null_values(self, column: str, replacement: Union[str, int, float]) -> int:
        """Fills null values in `column` with `replacement` in leads DataFrame

        :param column: Column name/s to search for null values
        :param replacement: Value to use to replace nulls
        :return: The number of values that has been replaced
        """
        num_nulls = self.leads_df[column].isnull().sum()

        self.leads_df = Transform.__replace_nulls__(self.leads_df, column, replacement)

        return num_nulls

    @staticmethod
    def __replace_nulls__(df: pd.DataFrame, column: str, replacement: Union[str, int, float]) -> pd.DataFrame:
        """Fills null values in `column` with `replacement`

        :param df: A pandas DataFrame
        :param column: Column name/s to search for null values
        :param replacement: Value to use to replace nulls
        :return: The DataFrame with replaced values
        """

        Transform.__guard_against_non_existent_columns__(df, [column])

        df[column] = df[column].fillna(replacement)

        return df

    def change_column_type(self, subset: Union[List[str], str], new_type: type):
        """Changes column/s data type from the leads and reviews DataFrames

        :param subset: Column name/s to be changed
        :param new_type: The new data type to be applied
        """
        Transform.__guard_against_non_existent_columns__(self.leads_df, subset)
        Transform.__guard_against_non_existent_columns__(self.reviews_df, subset)

        self.leads_df[subset] = self.leads_df[subset].astype(new_type)
        self.reviews_df[subset] = self.reviews_df[subset].astype(new_type)

    def create_courses_df(self, with_text_cleaning: bool = True) -> pd.DataFrame:
        """Creates a new DataFrame with courses information from leads and reviews DataFrames

        :param with_text_cleaning: Whether or not to clean the text columns
        :return: The courses DataFrame `courses_df`
        """
        keep_columns = ['course_id', 'course_title', 'course_description', 'course_category', 'center']

        courses_from_leads = self.leads_df[keep_columns].drop_duplicates('course_id')
        courses_from_reviews = self.reviews_df[keep_columns].drop_duplicates('course_id')

        self.courses_df = pd.merge(courses_from_leads, courses_from_reviews,
                                   left_on=keep_columns,
                                   right_on=keep_columns,
                                   how='outer')
        # Rename columns
        self.courses_df.rename(columns={'course_id': 'id', 'course_title': 'title',
                                        'course_description': 'description', 'course_category': 'category'},
                               inplace=True)

        # Remove duplicates
        self.courses_df.drop_duplicates('id', inplace=True)

        # Clean text columns
        if not with_text_cleaning:
            return self.courses_df

        def clean_text_column(text):
            if pd.isnull(text):
                return text

            return clean_text(text, exclude=['new_line'])

        self.courses_df['title'] = self.courses_df['title'].apply(clean_text_column)
        self.courses_df['description'] = self.courses_df['description'].apply(clean_text_column)
        self.courses_df['center'] = self.courses_df['center'].apply(clean_text_column)

        return self.courses_df

    def detect_courses_language(self) -> pd.DataFrame:
        """Add a language column in courses DataFrame based on description

        :return: The courses DataFrame `courses_df` with a new lang column
        """
        if 'lang' in self.courses_df.columns:
            return self.courses_df

        lang_detector = LangDetector()

        def get_lang(text):
            """Detects the language of a text

            :param text: Text of which we want to know the language
            :return str: Language ISO 639-1 code
            """
            if pd.isnull(text):
                return LangDetector.DEFAULT_LANGUAGE

            try:
                return lang_detector.iso_639_1_code(text)
            except ValueError:
                return LangDetector.DEFAULT_LANGUAGE

        self.courses_df['lang'] = self.courses_df['description'].apply(get_lang)

        return self.courses_df

    def remove_not_in_english_courses(self):
        """Removes courses that are not in English

        :return: The courses DataFrame `courses_df`
        """
        if 'lang' not in self.courses_df.columns:
            return self.courses_df

        not_in_english_courses = self.courses_df[self.courses_df['lang'] != 'en']

        # Remove courses
        self.courses_df.drop(self.courses_df[self.courses_df['lang'] != 'en'].index, inplace=True)

        # Remove lang column
        self.courses_df.drop('lang', axis=1, inplace=True)

        # Remove from leads and reviews
        self.reviews_df = self.reviews_df[~self.reviews_df['course_id'].isin(not_in_english_courses['id'].values)]
        self.leads_df = self.leads_df[~self.leads_df['course_id'].isin(not_in_english_courses['id'].values)]

        return self.courses_df

    def add_data_to_courses(self, m: int) -> pd.DataFrame:
        """Add additional data from leads and reviews to the courses DataFrame

        :param m: The minimum number of reviews required for the course to be listed
        :return: The courses DataFrame `courses_df`
        """
        # Add the rating average of each course
        self.courses_df['avg_rating'] = self.courses_df['id'].apply(average_rating, args=(self.reviews_df,))

        # Add the number of reviews received for each course
        self.courses_df['num_reviews'] = self.courses_df['id'].apply(num_reviews, args=(self.reviews_df,)).astype(int)

        # Add the weighted rating
        overall_avg_rating = all_avg_rating(self.courses_df)

        self.courses_df['weighted_rating'] = self.courses_df.apply(weighted_rating, axis=1,
                                                                   args=(overall_avg_rating, m,))

        # Add number of leads
        lead_counts = self.leads_df.groupby('course_id').count()['user_id']

        def number_of_leads(course_id: str) -> int:
            """Counts the number of leads generated by a course

            :param course_id: Course id
            :return int: The number of leads generated by a course
            """
            try:
                return lead_counts.loc[course_id]
            except KeyError:
                return 0

        self.courses_df['number_of_leads'] = self.courses_df['id'].apply(number_of_leads).astype(int)

        return self.courses_df

    def create_categories_df(self) -> pd.DataFrame:
        """Creates a new DataFrame of courses' categories

        :return: The categories DataFrame `categories_df`
        """
        # DataFrame creation
        categories = self.courses_df['category'].unique()
        self.categories_df = pd.DataFrame(categories, columns=['name']).reset_index()

        self.categories_df.rename(columns={'index': 'id'}, inplace=True)

        # To avoid category ids with 0 value
        self.categories_df['id'] = self.categories_df['id'].apply(lambda x: x + 1)

        # Add category id to courses DataFrame
        def category_id(category_name: str) -> int:
            """Searches for a category by name in the category data frame and returns its identifier

            :param category_name: Category name
            :return: The category id
            """
            return self.categories_df[self.categories_df['name'] == category_name]['id'].values[0]

        self.courses_df['category_id'] = self.courses_df['category'].apply(category_id)

        # Removes category name and keeps only the category id
        self.courses_df.drop('category', inplace=True, axis=1)

        return self.categories_df

    def create_course_content_similarity_matrix(self, sample_len: int = None) -> np.ndarray:
        """ Creates a course similarity matrix

        :param sample_len: Maximum number of courses used to create the matrix. If `None`, all courses in DataFrame
            will be used.
        :return: An m x m matrix representing the course similarities, where m is the number of courses.
            Example of a similarity matrix:

             [[0.99999994 0.         0.13075474 0.02665992]
              [0.         1.         0.00812627 0.00331377]
              [0.13075474 0.00812627 1.         0.0069054 ]
              [0.02665992 0.00331377 0.0069054  1.        ]]

            rows and columns represents a course, the elements in the matrix, represent the similarity between them.
        """
        course_content = self.courses_df['title'].str.cat(self.courses_df['description'], sep='. ')

        if not sample_len:
            course_content = course_content.values
        else:
            course_content = course_content.head(sample_len).values

        model = Pipeline([
            ('norm', TextNormalizer()),
            ('sim', Similarity())
        ])

        return model.fit_transform(course_content)

    def create_course_content_similarity_df(self, min_similarity: float = 0.5, sample_len: int = None) -> pd.DataFrame:
        """Creates a course similarity DataFrame from a similarity matrix

        :param min_similarity: Minimum similarity to be included in DataFrame
        :param sample_len: Maximum number of courses used to create the DataFrame. If `None`, all courses in
            courses DataFrame will be used.
        :return: a dataframe with following columns:
            a_course str: course id
            another_course str: course id
            similarity float: similarity between courses
        """
        sim_list = []
        sim_matrix = self.create_course_content_similarity_matrix(sample_len=sample_len)

        for idx, similarities in enumerate(sim_matrix):
            a_course_id = self.courses_df.iloc[idx]['id']

            for idx_sims, similarity in enumerate(similarities):
                if idx == idx_sims:
                    continue

                if similarity < min_similarity:
                    continue

                another_course_id = self.courses_df.iloc[idx_sims]['id']

                sim_list.append({'a_course_id': a_course_id,
                                 'another_course_id': another_course_id,
                                 'similarity': similarity})

        self.courses_content_sims_df = pd.DataFrame(sim_list)

        return self.courses_content_sims_df

    def create_ratings_user_item_matrix(self):
        """Creates a ratings user-item matrix DataFrame reviews"""
        user_items = self.reviews_df[['user_id', 'course_id', 'rating']]

        self.ratings_user_item_matrix = user_items.groupby(['user_id', 'course_id'])['rating'].max().unstack()

    def create_leads_user_item_matrix(self):
        """Creates a leads user-item matrix DataFrame reviews"""
        user_items = self.leads_df[['user_id', 'course_id']]

        user_item_matrix = user_items.groupby(['user_id', 'course_id'])['course_id'].max().unstack()
        user_item_matrix = user_item_matrix.where(user_item_matrix.isnull(), other=1)
        user_item_matrix = user_item_matrix.fillna(0)

        self.leads_user_item_matrix = user_item_matrix

    def requested_courses(self, user_id: str) -> np.ndarray:
        """Returns an array of courses ids to which the user has generated lead

        :param user_id: User id for which we want to find generated leads
        :return: An array of courses ids to which the user has generated lead
        """
        return self.leads_df[self.leads_df['user_id'] == user_id]['course_id'].values

    def course_course_recommendations(self, course_id: str, max_recs: int = 10) -> np.ndarray:
        """Returns an array of recommended courses based on leads generated in one course

        :param course_id: Course id for which we want to make the recommendations
        :param max_recs: Maximum number of recommendations
        :return numpy.array: Array of courses recommended based on generated leads in one course
        """
        users = np.array(self.leads_user_item_matrix[self.leads_user_item_matrix.loc[:, course_id] == 1].index)
        recs = np.array([])

        for user_id in users:
            user_courses = self.requested_courses(user_id)

            new_recs = user_courses[user_courses != course_id]
            recs = np.unique(np.concatenate([new_recs, recs], axis=0))

            if len(recs) > max_recs:
                break

        return recs[:max_recs]

    def create_course_course_recommendations_df(self):
        """Creates a course-course recommendations DataFrame"""
        recommendations = []
        for course in self.leads_user_item_matrix.columns:
            recs = self.course_course_recommendations(course)
            for rec in recs:
                recommendations.append({'course': course, 'recommended': rec})

        self.course_course_recs_df = pd.DataFrame(recommendations)

    def compress_leads_user_item_matrix(self):
        """Compress the leads user-item matrix to a csr_matrix format and creates a user requested courses map"""
        sparse_matrix = csr_matrix(self.leads_user_item_matrix, dtype='int8')

        for i in range(0, sparse_matrix.shape[0]):
            self.user_requested_courses_map[self.leads_user_item_matrix.index[i]] = sparse_matrix[i]

    @staticmethod
    def __guard_against_non_existent_columns__(df: pd.DataFrame, subset: Union[List[str], str] = None):
        """Raises a ValueError if any of the columns in `subset` is not in `df.columns`

        :param df: A pandas DataFrame
        :param subset: Column name/s
        :raises: ValueError
        """
        if subset is None:
            return
        if isinstance(subset, str):
            subset = [subset]

        if not all(column in df.columns for column in subset):
            not_in = np.setdiff1d(subset, df.columns, assume_unique=True)
            raise ValueError('[{}] does not exist in this DataFrame'.format(', '.join(not_in)))
