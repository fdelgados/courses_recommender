import numpy as np
import pandas as pd


def requested_courses(user_id: str, df: pd.DataFrame) -> np.ndarray:
    """Returns an array of courses ids to which the user has generated lead

    :param user_id: User id for which we want to find generated leads
    :param df: Leads DataFrame
    :return: An array of courses ids to which the user has generated lead
    """
    return df[df['user_id'] == user_id]['course_id'].values
