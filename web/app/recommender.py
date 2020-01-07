import numpy as np
from . import db
from sqlalchemy import text
from .models import CourseRepository


def find_neighbours(user_id: str) -> np.ndarray:
    """Find similar users in database

    :param user_id: Identifier of the user for whom we want to search neighbours
    :return: An array of similar user identifiers
    """
    query = '''SELECT another_user_id
                FROM users_similarities 
                WHERE a_user_id = :user_id
                ORDER BY similarity DESC'''

    result = db.engine.execute(text(query), user_id=user_id)

    return np.array([row['another_user_id'] for row in result])


class Recommender:
    """Makes courses recommendations"""

    def __init__(self):
        """Recommender constructor. Initializes the object."""

        self.by_leads = {}
        self.by_content = {}
        self.by_rating = {}
        self.by_number_of_leads = {}
        self.by_user = {}
        self.course_repository = CourseRepository()

    def make_recommendations_by_course(self, course_id, max_recommendations: int = 10) -> 'Recommender':
        """Make user interaction and content based recommendations

        :param course_id: Identifier of the course for which we want to find similar
        :param max_recommendations: Maximum number of recommendations
        :return: `Recommender` class
        """
        self.by_leads = self.course_repository.find_similar_by_leads(course_id, max_recommendations)
        self.by_content = self.course_repository.find_similar_by_content(course_id, max_recommendations)

        return self

    def make_rank_recommendations(self, category_id: int = None, exclude_course_id: str = None,
                                  max_recommendations: int = 10) -> 'Recommender':
        """Make rank based recommendations

        :param category_id: Category identifier if we want to make recommendations of this category
        :param exclude_course_id: Course identifier if we want to exclude it from the recommendations
        :param max_recommendations: Maximum number of recommendations
        :return: `Recommender` class
        """
        self.by_rating = self.course_repository.find_sorted_by_rating(category=category_id,
                                                                      max_rows=max_recommendations,
                                                                      exclude=exclude_course_id)
        self.by_number_of_leads = self.course_repository.find_sorted_by_leads(category=category_id,
                                                                              max_rows=max_recommendations,
                                                                              exclude=exclude_course_id)

        return self

    def make_recommendations_by_user(self, user_id: str = None, max_recommendations: int = 10) -> 'Recommender':
        """Makes neighbourhood based recommendations

        :param user_id: User identifier to which we want to recommend courses
        :param max_recommendations: Maximum number of recommendations
        :return: `Recommender` class
        """
        if not user_id:
            return self

        user_courses = self.course_repository.find_by_user_leads(user_id)
        user_courses_ids = np.array(list(user_courses.keys()))

        if len(user_courses_ids) == 0:
            return self

        rec_courses_ids = np.array([])
        neighbours_courses = {}

        similar_users = find_neighbours(user_id)
        for similar_user_id in similar_users:
            neighbour_courses = self.course_repository.find_by_user_leads(similar_user_id)
            neighbours_courses.update(neighbour_courses)

            new_recs = np.setdiff1d(np.array(list(neighbour_courses.keys())), user_courses_ids, assume_unique=True)
            rec_courses_ids = np.unique(np.concatenate([new_recs, rec_courses_ids], axis=0))

            if len(rec_courses_ids) > max_recommendations:
                break

        if len(rec_courses_ids) == 0:
            return self

        self.by_user = {course_id: course for (course_id, course) in neighbours_courses.items()
                        if course_id in rec_courses_ids}

        return self
