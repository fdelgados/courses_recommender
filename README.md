# Courses recommendation

A tool to recommend courses.

## Table of Contents
1. [Introduction](#introduction)
2. [Dependencies](#dependencies)
3. [References](#references)

<a id="introduction"></a>
## Introduction

[Emagister](https://www.emagister.com) is a company whose objective is to be a meeting point for students and course providers and they aim to do so by helping people find the right training. That's why the recommender system is one of the most important parts of the web. So the main motivation of this project is to improve the current recommendation system.

### Data used for the project

The data that will be worked on in this project are real data extracted from the [Emagister UK](https://www.emagister.co.uk) database. As an employee of Emagister, I requested authorization from the company to use the data, after consulting with our lawyers, the company permitted me. 

For security and legal reasons, user data is encrypted. 

### The recommender system

The recommendations are based on the following four methods of recommendations:

* Knowledge based recommendations
* Content based filtering
* Neighborhood based collaborative filtering
* Model based collaborative filtering

The project is divided in four parts:

1. ETL pipeline (this notebook)
2. Exploratory data analysis
3. Make recommendations
4. A demo web application

#### ETL pipeline

The pipeline retrieves raw data from database, then performs the data wrangling process on this data and finally loads the resulting formatted data to database and files, ready to be used in the web application.

I have taken the code written in this section and arranged into several classes and a script, which allows you to automate the ETL process.
To execute the ETL pipeline script, run the following commands:

```
$ cd etl/
$ python pipeline.py <username> <password>1
```
![ETL Pipeline](https://github.com/fdelgados/courses_recommender/blob/master/img/etl_console.png)

#### Exploratory data analysis

Once the data is cleaned, is time to perform an exploratory data analysis. I will search for patterns and trends in data and I will create visualizations for this data as well.

#### Make recommendations

After the exploratory data analysis, is time to play around with structures created in the first part and trying to make recommendations.

#### Demo web

The observations and models derived from the ETL phase and analysis of the project, have been put into practice in a web application that can be accessed [here](https://courses-recommender.herokuapp.com/).
<a id="dependencies"></a>

## Dependencies
To run this project properly, you need the following:

* Python >=3.5
* numpy 1.18.1
* pandas 0.24.2
* scikit-learn 0.20.3
* scipy 1.2.1
* sqlalchemy 1.3.2
* matplotlib 3.0.3
* halo 0.0.28 (Spinner for terminal. [PyPi](https://pypi.org/project/halo/))
* pymysql 0.9.3 (Python MySQL client library. [PyPi](https://pypi.org/project/PyMySQL/))

In addition you need to install `texcptulz`, a library designed to transform the raw ingested text into a form that is ready for calculation and modelling.
This library has been developed by me for this project. To install `texcptulz` run the following command:

`pip install texcptulz`

More information [here](https://pypi.org/project/texcptulz/).

<a id="references"></a>
## References

### Books
* Bengford, Bilbro & Ojeda (2018) [Applied Text Analysis with Python](http://shop.oreilly.com/product/0636920052555.do). O'Reilly Media Inc.
* Segaran (2007) [Programming Collective Intelligence](http://shop.oreilly.com/product/9780596529321.do). O'Reilly Media Inc.
* Phillips (2019) [Python 3 Object-Oriented Programming. Third Edition](https://www.packtpub.com/application-development/python-3-object-oriented-programming-third-edition). Packt Publishing Ltd.
* Grinberg (2014) [Flask Web Development. Developing Web Applications with Python](http://shop.oreilly.com/product/0636920031116.do). O'Reilly Media Inc.

### Online resources
* [Flask Documentation](https://flask.palletsprojects.com/en/1.1.x/)
* [Jinja Documentation](https://jinja.palletsprojects.com/en/2.10.x/)
* [SQLAlchemy Documentation](https://docs.sqlalchemy.org/en/13/)
* [Article about Customer Ratings](https://baymard.com/blog/sort-by-customer-ratings)
