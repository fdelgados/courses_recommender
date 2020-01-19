import numpy as np
import pandas as pd
import pickle

from scipy.sparse import csr_matrix
from sklearn.pipeline import Pipeline

from .db_service import DbService

from txtools.normalizer import TextNormalizer
from txtools.similarity import Similarity


class Model(DbService):
    def __init__(self, db_user: str, db_password: str, db_name: str = None, db_host: str = None):
        self.courses_df = None
        self.leads_df = None
        self.courses_content_sims_df = None
        self.leads_user_item_matrix = None
        self.user_requested_courses_map = {}
        self.course_course_recs_df = None

        super().__init__(db_user, db_password, db_name, db_host)

    def retrieve_courses(self) -> pd.DataFrame:
        """Retrieve courses from database
        :return: Courses DataFrame
        """
        if self.courses_df is None:
            self.courses_df = pd.read_sql_query('SELECT * FROM courses', con=self.connection())

        return self.courses_df

    def retrieve_leads(self) -> pd.DataFrame:
        """Retrieve leads from database
        :return: Leads DataFrame
        """
        if self.leads_df is None:
            self.leads_df = pd.read_sql_query('SELECT * FROM clean_leads ORDER BY created_on DESC',
                                              con=self.connection())
        return self.leads_df

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

        self.retrieve_courses()

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
        self.retrieve_courses()
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

    def save_course_content_similarities(self):
        """Saves courses content similarities DataFrame to database"""
        return

        connection = self.connection()

        sql_drop = 'DROP TABLE IF EXISTS `courses_similarities`'
        connection.execute(sql_drop)

        sql_create = """CREATE TABLE `courses_similarities` (
          `a_course_id` VARCHAR(9) NOT NULL,
          `another_course_id` VARCHAR(9) NOT NULL,
          `similarity` DOUBLE NOT NULL,
          PRIMARY KEY (`a_course_id`, `another_course_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        connection.execute(sql_create)

        # Save course similarities to database
        self.courses_content_sims_df.to_sql('courses_similarities', con=connection,
                                            if_exists='append', index=False)

    def create_leads_user_item_matrix(self):
        """Creates a leads user-item matrix DataFrame reviews"""

        self.retrieve_leads()
        user_items = self.leads_df[['user_id', 'course_id']]

        user_item_matrix = user_items.groupby(['user_id', 'course_id'])['course_id'].max().unstack()
        user_item_matrix = user_item_matrix.where(user_item_matrix.isnull(), other=1)
        user_item_matrix = user_item_matrix.fillna(0)

        self.leads_user_item_matrix = user_item_matrix

    def compress_leads_user_item_matrix(self):
        """Compress the leads user-item matrix to a csr_matrix format and creates a user requested courses map"""
        sparse_matrix = csr_matrix(self.leads_user_item_matrix, dtype='int8')

        for i in range(0, sparse_matrix.shape[0]):
            self.user_requested_courses_map[self.leads_user_item_matrix.index[i]] = sparse_matrix[i]

    def save_user_courses_map(self, file_name: str):
        with open(file_name, 'wb') as file:
            pickle.dump(self.user_requested_courses_map, file)

    def requested_courses(self, user_id: str) -> np.ndarray:
        """Returns an array of courses ids to which the user has generated lead

        :param user_id: User id for which we want to find generated leads
        :return: An array of courses ids to which the user has generated lead
        """
        self.retrieve_leads()

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

    def save_course_course_recommendations(self):
        """Saves courses recommendations DataFrame to database"""
        conn = self.connection()

        sql_drop = 'DROP TABLE IF EXISTS `recommended_courses_by_leads`'
        conn.execute(sql_drop)

        sql_create = """CREATE TABLE `recommended_courses_by_leads` (
          `course` VARCHAR(9) NOT NULL,
          `recommended` VARCHAR(9) NOT NULL,
          PRIMARY KEY (`course`, `recommended`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        conn.execute(sql_create)

        # Save recommendations to database
        self.course_course_recs_df.to_sql('recommended_courses_by_leads', con=conn,
                                          if_exists='append', index=False)
