import numpy as np
import os
from typing import Dict
from .models import CourseRepository
import pickle


def find_similar_users(user_id: str, sparse_user_item_dict: Dict, min_similarity: int = 1):
    """
    Creates an array of similar users based on leads generated on the same courses

    :param user_id: User id for which we want to find similar users
    :param sparse_user_item_dict: Leads user-item matrix
    :param min_similarity: Minimum similarity between users to be listed

    :return numpy.array: Array of similar users sorted by similarity
    """

    user_courses = np.array(sparse_user_item_dict[user_id].todense())[0]

    similarities = dict()

    for another_user_id, another_user_courses in sparse_user_item_dict.items():
        if user_id == another_user_id:
            continue

        similarity = np.dot(user_courses, np.array(another_user_courses.todense())[0])
        if similarity < min_similarity:
            continue

        similarities[another_user_id] = similarity

    sorted_similarities = sorted(similarities.items(), key=lambda item: item[1], reverse=True)

    return np.array([user_id for (user_id, similarity) in sorted_similarities])


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

        user_map_courses_file = '{}/../data/user_courses_map.pickle'.format(os.path.dirname(os.path.abspath(__file__)))

        with open(user_map_courses_file, 'rb') as filename:
            user_courses_map = pickle.load(filename)

        similar_users = find_similar_users(user_id, user_courses_map)
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
