import numpy as np
import pandas as pd
from typing import Optional


def average_rating(course_id: str, df: pd.DataFrame) -> Optional[float]:
    """Computes the average rating of a course

    :param course_id: Course id
    :param df: Courses DataFrame
    :return: The average rating of a course or np.nan if the course has no rating
    """
    df = df[df['course_id'] == course_id]

    if df.shape[0] == 0:
        return np.nan

    return df['rating'].mean()


def num_reviews(course_id: str, df: pd.DataFrame) -> int:
    """Counts the number of reviews of a course

    :param course_id: Course id
    :param df: Courses DataFrame
    :return: The number of reviews of a course
    """
    df = df[df['course_id'] == course_id]

    return df.shape[0]


def all_avg_rating(df: pd.DataFrame) -> float:
    """Computes the average rating of all courses (C)

    :param df: Courses DataFrame
    :return: The average rating of all courses in DataFrame
    """
    df = df[~df['avg_rating'].isnull()]

    return df['avg_rating'].mean()


def weighted_rating(row, C: float, m: int) -> float:
    """Computes the weighted rating of a course

    :param row DataFrame row: A DataFrame row representing a course
    :param C: The average rating of all the courses in the dataset
    :param m: The minimum number of reviews required for the course to be listed

    :return: The average rating of all courses in DataFrame
    """
    v = row['num_reviews']
    R = row['avg_rating']

    if v == 0 and np.isnan(R):
        return np.nan

    wr = (v * R / (v + m)) + (m * C / (v + m))

    return wr
