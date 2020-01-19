import math
import datetime
from abc import ABC, abstractmethod
from typing import Dict, Any, Union, List
from sqlalchemy.sql import text
from . import db


class Paginator:
    """Paginator class. Allows to make queries to database"""

    def __init__(self, page: int, items_per_page: int):
        """Paginator constructor

        :param page: Current page number
        :param items_per_page: Number of items per page
        """
        self.current_page = page
        self.items_per_page = items_per_page
        self.offset = (page - 1) * items_per_page
        self.row_count = 0
        self.page_count = 0

    def set_counts(self, query: str, **params):
        """Performs a query to database and sets the number of rows and the number o pages

        :param query: Query to database
        :param params: Query parameters
        """
        result = db.engine.execute(text(query), **params)

        self.row_count = result.rowcount
        self.page_count = int(math.ceil(self.row_count / self.items_per_page))


class Repository(ABC):
    """Repository base class. Performs queries to database and builds the response"""

    def __init__(self, paginator: Paginator = None):
        """Repository constructor

        :param paginator: Paginator class. If None, queries wont be paginated
        """
        self.paginator = paginator

    def paginated_query(self, query: str, **params):
        """Adds limit and offset to a query and counts the total rows

        :param query: Query to database
        :param params: Query parameters
        :return: Query to database with limit and offset added
        """
        if self.paginator:
            self.paginator.set_counts(query, **params)
            query = '{} limit {}, {}'.format(query, self.paginator.offset, self.paginator.items_per_page)

        return query

    def execute(self, query: str, **kwargs) -> 'ResultProxy':
        """Executes a query to database

        :param query: Query to database
        :param kwargs: Query parameters
        :return: db.engine.ResultProxy
        """
        if 'limit' in kwargs and kwargs['limit']:
            query = '{} LIMIT :limit'.format(query)
        else:
            query = self.paginated_query(query, **kwargs)

        return db.engine.execute(text(query), **kwargs)

    @abstractmethod
    def build_response(self, query: str, **kwargs) -> Any:
        """Creates a collection of entities from a query

        :param query: Query to database
        :param kwargs: Query parameters
        """
        pass


class Category:
    """Category entity """

    def __init__(self, id: int, name: str):
        """Category constructor. Initializes the object

        :param id: Category identifier
        :param name: Category name
        """
        self.id = id
        self.name = name
        self.number_of_leads = 0
        self.weighted_rating = 0.0

    def set_number_of_leads(self, number_of_leads: int):
        """Sets the total number of leads in this category

        :param number_of_leads: The number of leads
        """
        self.number_of_leads = number_of_leads

    def set_weighted_rating(self, total_weighted_rating: float):
        """Sets the weighted rating of all courses in this category

        :param total_weighted_rating: The weighted rating
        """
        self.weighted_rating = total_weighted_rating


class CategoryRepository(Repository):
    """Category repository. Manages the queries that concern the categories"""

    def find_all(self, max_rows: int = None) -> Dict[int, Category]:
        """Returns a collection of categories

        :param max_rows: Maximum number of categories to retrieve
        :return: A collection of categories
        """
        query = '''SELECT cat.id, cat.name, count(c.category_id) AS num_courses
                     FROM categories cat
                     JOIN courses c ON cat.id = c.category_id
                     GROUP BY cat.id, cat.name
                     ORDER BY num_courses DESC'''

        return self.build_response(query, limit=max_rows)

    def find_popular(self, max_rows: int = None, min_weighted_rating: float = 7.0) -> Dict[int, Category]:
        """Returns a collection of most popular categories considering the weighted rating of their courses

        :param max_rows: Maximum number of categories to retrieve
        :param min_weighted_rating: Minimum weighted rating to be listed
        :return: A collection of popular categories
        """
        query = '''SELECT cat.id, cat.name, SUM(c.number_of_leads) AS cat_number_of_leads,
                    AVG(c.weighted_rating) AS cat_weighted_rating
                     FROM categories cat
                     JOIN courses c ON cat.id = c.category_id
                     WHERE c.number_of_leads >= 1
                     GROUP BY cat.id, cat.name
                     HAVING cat_weighted_rating >= :min_weighted_rating
                     ORDER BY cat_number_of_leads DESC, cat_weighted_rating DESC'''

        return self.build_response(query, limit=max_rows, min_weighted_rating=min_weighted_rating)

    def find(self, category_id: int) -> Category:
        """Returns the category entity with the supplied identifier

        :param category_id: The category identifier
        :return: A category
        """
        query = '''SELECT cat.id, cat.name, SUM(c.number_of_leads) AS cat_number_of_leads,
                    AVG(c.weighted_rating) AS cat_weighted_rating
                     FROM categories cat
                     JOIN courses c ON cat.id = c.category_id
                     WHERE cat.id = :category_id
                     GROUP BY cat.id, cat.name'''

        categories = self.build_response(query, category_id=category_id)

        if len(categories) == 0:
            raise ValueError('There is no category with the identifier {}'.format(category_id))

        return list(categories.values())[0]

    def build_response(self, query: str, **kwargs) -> Dict[int, Category]:
        """Executes the query to database and builds a collection of categories from the response

        :param query: Query to database
        :param kwargs: Query parameters
        :return: A collection of categories
        """
        categories = {}

        result = self.execute(query, **kwargs)

        for row in result:
            category = Category(row['id'], row['name'])
            category.set_number_of_leads(row['cat_number_of_leads'])
            category.set_weighted_rating(row['cat_weighted_rating'])

            categories[category.id] = category

        return categories


class Course:
    """Course entity. Contains all information about a course"""

    def __init__(self, id: str, title: str, category: Category, center: str):
        """Course constructor. Initializes the object.

        :param id: Course id
        :param title: Course title
        :param category: Course category
        :param center: Course center
        """
        self.id = id
        self.title = title
        self.description = None
        self.category = category
        self.center = center
        self.number_of_reviews = None
        self.weighted_rating = None
        self.number_of_leads = None

    def set_number_of_leads(self, number_of_leads: int):
        """Sets the number of leads

        :param number_of_leads: The number of leads
        """
        self.number_of_leads = number_of_leads

    def set_number_of_reviews(self, number_of_reviews: int):
        """Sets the number of reviews

        :param number_of_reviews: The number of reviews
        """
        self.number_of_reviews = number_of_reviews

    def set_weighted_rating(self, weighted_rating: float):
        """Sets the weighted rating

        :param weighted_rating: The weighted rating
        """
        self.weighted_rating = weighted_rating

    def set_description(self, description: str):
        """Sets the course description

        :param description: The cours description
        """
        self.description = description

    @property
    def category_name(self) -> str:
        """Returns the course category name

        :return: The course category name
        """
        return self.category.name

    @property
    def category_id(self) -> int:
        """Returns the course category identifier

        :return: The course category identifier
        """
        return self.category.id


class CourseRepository(Repository):
    """Category repository. Manages the queries that concern the courses"""

    def find_all_by(self, category: int = None,
                    max_rows: int = None,
                    exclude: str = None,
                    min_number_of_leads: int = 1,
                    min_weighted_rating: float = 7.0,
                    order_by: Union[Dict, List] = None) -> Dict[str, Course]:
        """Returns a collection of courses that meet the parameters provided

        :param category: Category identifier
        :param max_rows: Maximum number of courses to retrieve. If it's None, all courses will be retrieved
        :param exclude: Course identifier to exclude from search
        :param min_number_of_leads: Minimum number of leads to be listed
        :param min_weighted_rating: Minimum weighted rating to be listed
        :param order_by: Columns by which the result will be sorted. It can be a list of columns and the result sorting
            will be in ascending order; or a dictionary whose keys must be the column names and the values must be the
            sorting direction of that column. Ex: {'num_reviews': 'ASC', 'weighted_rating': 'DESC'}
        :return: A collection of courses
        """
        query = '''SELECT c.id, c.title, c.description, c.category_id, cat.name AS category_name, c.center,
                        c.number_of_leads, c.num_reviews, ROUND(c.weighted_rating, 2) AS weighted_rating
                   FROM courses c 
                   JOIN categories cat ON c.category_id = cat.id
                   WHERE c.number_of_leads >= :min_number_of_leads
                   AND c.weighted_rating >= :min_weighted_rating
                   '''

        if category:
            query = '{} AND c.category_id = :category_id'.format(query)

        if exclude:
            query = '{} AND c.id <> :course_id'.format(query)

        query = '{} GROUP BY c.id'.format(query)

        if order_by:
            if isinstance(order_by, list):
                order_by = ', '.join(order_by)
            elif isinstance(order_by, dict):
                order_by = ', '.join([key + ' ' + value for (key, value) in order_by.items()])

            query = '{} ORDER BY {}'.format(query, order_by)

        return self.build_response(query, limit=max_rows,
                                   category_id=category,
                                   course_id=exclude,
                                   min_number_of_leads=min_number_of_leads,
                                   min_weighted_rating=min_weighted_rating)

    def find_sorted_by_leads(self, category: int = None,
                             max_rows: int = None,
                             exclude: str = None) -> Dict[str, Course]:
        """Returns a collection of courses sorted by number of leads

        :param category: Category to which the courses belong
        :param max_rows: Maximum number of courses to retrieve. The limit in the select query
        :param exclude: Course ids excluded from search
        :return: A collection of courses
        """
        return self.find_all_by(category=category,
                                max_rows=max_rows,
                                exclude=exclude,
                                order_by={'number_of_leads': 'DESC', 'weighted_rating': 'DESC', 'num_reviews': 'DESC'})

    def find_sorted_by_rating(self, category: int = None,
                              max_rows: int = None,
                              exclude: str = None) -> Dict[str, Course]:
        """Returns a collection of courses sorted by weighted rating

        :param category: Category to which the courses belong
        :param max_rows: Maximum number of courses to retrieve. The limit in the select query
        :param exclude: Course ids excluded from search
        :return: A collection of courses
        """
        return self.find_all_by(category=category,
                                max_rows=max_rows,
                                exclude=exclude,
                                order_by={'weighted_rating': 'DESC', 'num_reviews': 'DESC', 'number_of_leads': 'DESC'})

    def find_similar_by_leads(self, course_id: str, max_rows: int = None) -> Dict[str, Course]:
        """Returns a collection of recommended courses. The courses have in common that the same user generated a
            lead in them.

        :param course_id: Course identifier from which we want to look for the similar
        :param max_rows: Maximum number of courses to retrieve. The limit in the select query
        :return: A collection of similar courses
        """

        query = '''SELECT r.recommended AS id, c.title, c.description, c.category_id, cat.name AS category_name,
                    c.center, c.number_of_leads, c.num_reviews, ROUND(c.weighted_rating, 2) AS weighted_rating
                FROM recommended_courses_by_leads r
                JOIN courses c ON r.recommended = c.id
                JOIN categories cat ON cat.id = c.category_id
                WHERE r.course = :course_id
                ORDER BY number_of_leads DESC, weighted_rating DESC, num_reviews DESC'''

        return self.build_response(query, course_id=course_id, limit=max_rows)

    def find_similar_by_content(self, course_id: str, max_rows: int = None) -> Dict[str, Course]:
        """Returns a collection of courses with similar title and description to a course, sorted by similarity

        :param course_id: Course identifier from which we want to look for the similar
        :param max_rows: Maximum number of courses to retrieve. The limit in the select query
        :return: A collection of similar courses
        """
        query = '''SELECT cs.another_course_id AS id, c.title, c.description, c.category_id, cat.name AS category_name,
                    c.center, c.number_of_leads, c.num_reviews, ROUND(c.weighted_rating, 2) AS weighted_rating 
                FROM courses_similarities cs
                JOIN courses c ON cs.another_course_id = c.id
                JOIN categories cat ON cat.id = c.category_id
                WHERE cs.a_course_id = :course_id
                ORDER BY cs.similarity DESC'''

        return self.build_response(query, course_id=course_id, limit=max_rows)

    def find_requested_by_user(self, user_id: str) -> Dict[str, Course]:
        """Returns a collection of courses to which a user has generated a lead

        :param user_id: User identifier that generated the leads
        :return: A collection  of courses to which the same user has generated a lead
        """
        query = '''SELECT c.id, c.title, c.description, c.center, c.category_id, cat.name AS category_name,
                   c.number_of_leads, c.num_reviews, ROUND(c.weighted_rating, 2) AS weighted_rating
                FROM clean_leads l
                JOIN courses c ON l.course_id = c.id
                JOIN categories cat ON c.category_id = cat.id
                WHERE user_id = :user_id'''

        return self.build_response(query, user_id=user_id)

    def find(self, course_id: str) -> Course:
        """Returns the course entity with the supplied identifier

        :param course_id: The course identifier
        :return: A course
        """
        query = '''SELECT c.id, title, description, center, category_id, c.center,
                cat.name AS category_name,  c.number_of_leads,
                c.weighted_rating, c.num_reviews
                FROM courses c JOIN categories cat ON c.category_id = cat.id
                WHERE c.id = :course_id
                GROUP BY c.id'''

        courses = self.build_response(query, course_id=course_id)

        if len(courses) == 0:
            raise ValueError('There is no course with the identifier {}'.format(course_id))

        return list(courses.values())[0]

    def build_response(self, query: str, **kwargs) -> Dict[str, Course]:
        """Executes the query to database and builds a collection of courses from the response

        :param query: Query to database
        :param kwargs: Query parameters
        :return: A collection of courses
        """
        courses = {}
        result = self.execute(query, **kwargs)

        for row in result:
            course = Course(row['id'],
                            row['title'],
                            Category(row['category_id'], row['category_name']),
                            row['center'])

            if row['description']:
                course.set_description(row['description'])

            course.set_weighted_rating(row['weighted_rating'])
            course.set_number_of_reviews(row['num_reviews'])
            course.set_number_of_leads(row['number_of_leads'])

            courses[course.id] = course

        return courses


class Lead:
    """Lead entity. Contains all information about a lead and the course"""

    def __init__(self, user_id: str, course: Course, created_on: str = None):
        """Lead constructor. Initializes the object

        :param user_id: User identifier that generated the lead
        :param course: Course to which the lead has been generated
        :param created_on: Lead creation date
        """
        self.user_id = user_id
        self.course = course
        self.created_on = datetime.datetime.now() if created_on is None else created_on

    @property
    def course_id(self) -> str:
        """Returns the course id

        :return: Course id
        """
        return self.course.id


class LeadRepository(Repository, ABC):
    """Lead repository. Manages the queries that concern the leads"""

    def save(self, lead: Lead):
        """Persists a lead into the database

        :param lead: The lead to persist
        """
        insert_sql = '''INSERT INTO leads (user_id, course_id, course_title, 
                        course_description, center, course_category, created_on)
                        VALUES (:user_id, :course_id, :course_title, :course_description,
                        :center, :course_category, :created_on)'''

        self.execute(insert_sql,
                     user_id=lead.user_id,
                     course_id=lead.course_id,
                     course_title=lead.course.title,
                     course_description=lead.course.description,
                     center=lead.course.center,
                     course_category=lead.course.category_name,
                     created_on=lead.created_on)
