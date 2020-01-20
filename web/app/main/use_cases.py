from ..models import CourseRepository, CategoryRepository, Paginator
from ..models import Lead, LeadRepository
from ..recommender import Recommender
from typing import Dict, Optional
import hashlib


def hash_user_email(user_email: str) -> Optional[str]:
    """Hashes an email

    :param user_email: The user email
    :return: A user hash
    """
    if user_email is None:
        return None

    return hashlib.md5(user_email.encode()).hexdigest()


class RetrieveCourseCatalogCommand:
    """Request command containing query parameters"""

    SORT_LEADS = 'leads'
    SORT_RATING = 'rating'

    def __init__(self, page: int, sort_by: str, category: int):
        """Initializes the command
        :param page: Page number
        :param sort_by: rating|leads
        :param category: Category identifier
        """

        if sort_by != self.SORT_LEADS and sort_by != self.SORT_RATING:
            raise ValueError('sort_by must be {} or {}.'.format(self.SORT_LEADS, self.SORT_RATING))

        self.page = int(page)
        self.sort_by = sort_by
        self.category = category


class RetrieveCourseCatalog:
    """Use case class to retrieve the course catalog"""

    @staticmethod
    def execute(command: RetrieveCourseCatalogCommand) -> Dict:
        """ Retrieve the course catalog, a list of categories, the current category from database.
            It also returns information to create the paginator

        :param command: The use case request command containing query parameters
        :return: A dictionary with data to be passed to view
        """
        courses = []
        page = command.page
        sort_by = command.sort_by
        category_id = None

        if command.category is not None:
            category_id = int(command.category)

        courses_per_page = 20
        paginator = Paginator(page, items_per_page=courses_per_page)
        course_repository = CourseRepository(paginator)

        if sort_by == RetrieveCourseCatalogCommand.SORT_LEADS:
            courses = course_repository.find_sorted_by_leads(category_id)
        if sort_by == RetrieveCourseCatalogCommand.SORT_RATING:
            courses = course_repository.find_sorted_by_rating(category_id)

        prev_page = page - 1 if page >= 1 else None
        next_page = page + 1 if page < paginator.page_count else None

        category_repository = CategoryRepository()

        categories = category_repository.find_popular(max_rows=10)

        category_name = None
        if category_id is not None:
            selected_category = category_repository.find(category_id)
            category_name = selected_category.name

        return {'courses': courses,
                'categories': categories,
                'category_id': category_id,
                'category_name': category_name,
                'current_page': page,
                'total_pages': paginator.page_count,
                'next_page': next_page,
                'prev_page': prev_page,
                'sort_by': sort_by}


class RetrieveCourseDataCommand:
    """Request command containing the course's identifier and user's identifier"""

    def __init__(self, course_id: int, user_id: str = None):
        """Initializes the command

        :param course_id: Course's identifier
        :param user_id: User's identifier
        """
        self.course_id = course_id
        self.user_id = user_id


class RetrieveCourseData:
    """Use case class to retrieve data from a course"""

    @staticmethod
    def execute(command: RetrieveCourseDataCommand):
        """Retrieves data from a course, recommendations based on it and rank based recommendations

        :param command: The use case request command containing the course identifier
        :return: A dictionary with the course and recommendations
        """
        course_repository = CourseRepository()

        course = course_repository.find(str(command.course_id))
        recommender = Recommender()
        recommender.make_recommendations_by_course(course.id).make_rank_recommendations(course.category_id,
                                                                                        str(course.id))
        if command.user_id:
            recommender.make_recommendations_for_user(command.user_id)

        return {'course': course,
                'recommendations': recommender}


class PlaceAnInfoRequestCommand:
    """Request command containing the user email and the course identifier"""

    def __init__(self, course_id: str, email: str):
        """Initializes the command

        :param course_id: Course identifier
        :param email: User email
        """
        self.course_id = course_id
        self.email = email


class PlaceAnInfoRequest:
    """Use case class to place an information request"""

    @staticmethod
    def execute(command: PlaceAnInfoRequestCommand) -> Dict:
        """Places an information request and returns recommendations based on the course and the user

        :param command: The use case request command containing the user email and the course identifier
        :return: A dictionary with recommendations
        """
        course_repository = CourseRepository()
        lead_repository = LeadRepository()

        course = course_repository.find(command.course_id)
        # Hashing the user email
        user_id = hash_user_email(command.email)
        lead = Lead(user_id, course)

        success = True
        recommender = Recommender()
        try:
            lead_repository.save(lead)
            recommender.make_recommendations_by_course(course.id)
        except Exception:
            success = False

        return {
            'success': success,
            'user_id': user_id,
            'course_id': course.id,
            'course_title': course.title,
            'recommendations': recommender
        }


class RetrieveHomeRecommendationsCommand:
    """Request command containing the user's identifier"""

    def __init__(self, user_id: str = None):
        """Initializes the command

        :param user_id: User's identifier
        """
        self.user_id = user_id


class RetrieveHomeRecommendations:
    """Use case class to make recommendations to a user"""

    @staticmethod
    def execute(command: RetrieveHomeRecommendationsCommand) -> Dict:
        """Makes recommendations to a specific user

        :param command: Request command containing the user identifier
        :return: A dictionary with recommendations to that user
        """
        recommendations = Recommender()

        recommendations.make_rank_recommendations().make_recommendations_for_user(command.user_id)

        category_repository = CategoryRepository()

        categories = category_repository.find_popular(max_rows=10)

        return {'recommendations': recommendations, 'categories': categories}


class RetrieveCategories:
    """Use case class to retrieve a dictionary of popular categories based on the number of leads"""

    @staticmethod
    def execute() -> Dict:
        """Retrieves a list of popular categories

        :return: A dictionary with popular categories
        """
        category_repository = CategoryRepository()

        return {'categories': category_repository.find_popular(min_weighted_rating=0.0)}
