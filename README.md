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

The recommendations are based on the following three methods of recommendations:

* Knowledge Based Recommendations
* Collaborative Filtering Based Recommendations
* Content Based Recommendations

The project is divided in two parts:

1. ETL pipeline
2. A demo web application

#### ETL pipeline

The pipeline consists on extract data from database re

#### Demo web

The observations and models derived from the ETL phase and analysis of the project, have been put into practice in a web application that can be accessed [here](https://courses-recommender.herokuapp.com/).
The application code is in this same repository, in the [/web](https://github.com/fdelgados/courses_recommender/tree/master/web) folder

<a id="dependencies"></a>
## Dependencies
To run this project properly, you need the following:

* Python >=3.5
* numpy 1.18.1
* pandas 0.24.2
* scikit-learn 0.20.3
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
