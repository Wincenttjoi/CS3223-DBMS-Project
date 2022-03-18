# CS3223-DBMS-Project


## Introduction

This project is based on SimpleDB, a multi-user transactional database server written in Java. SimpleDB is developed by Edward Sciore (Boston College), and the code is an integral part of a textbook Database Design and Implementation (Second Edition) he published with Springer. You should be able to access an e-copy from the NUS Library.

Unlike a full-fledged DBMS like MySQL or PostgreSQL, SimpleDB is intended as a teaching tool to facilitate the learning experience of a database system internal course. As such, the system implements only the basic functionalities of a “complete” database system. For example, it supports a very limited subset of SQL (and JDBC) and algorithms, offers little or no error checking, and is not designed for optimal performance/efficiency. However, the code is very well structured so that it is relatively easy to learn, use and extend SimpleDB.


## Goals

This project will focus primarily on query processing (and related topics, e.g., parser, indexing, etc) for a single user. [Things that are not considered: disk/memory management, transaction management (concurrency control and logging) and failures (recovery management), and multi-user setting usage.] 


## Specifications

1.     Lab 0: Set up SimpleDB & create a student database

2.     Lab 1: Support for non-equality predicates (Due: 28 Jan 2022)

3.     Lab 2: Support for hash index and B+-tree index (Due: 7 Feb 2022)

4.     Lab 3: Support for order by clause and sorting (14 Feb 2022)

5.     Lab 4: Support for nested-loops join, sort-merge join and index-based join (21 Feb 2022)

6.     Lab 5: Support for partition-based (or hash) join, and aggregates (SUM, COUNT, AGV, MIN, MAX) with/without group by clause (using sort-based implementation of group by operator)

7.     Lab 6: Support for DISTINCT and displaying the query plan  

8.     Lab 7: Integrate all features into SimpleDB+


## Bonus Implemented

1. Support range search for merge join, index join and index select.
2. Avoiding redundant select plan execution after index select and index joins, while supporting for multiple indexes in the predicate in a single query
3. Support float data type which supports group by avg
4. Support joining an empty table with no tuples to another table


## Final Reports

[Final Implementation and Lab Reports](https://docs.google.com/document/d/1WAmOkKuBpDju0Z51KAZP94Hqbe4x9rgB-1L3mXFdGY8/edit?usp=sharing) - Changes made to each lab

[Experimental Write-up Reports](https://docs.google.com/document/d/19m34kx76DyV-eDJPENI--87HD6bc94khoqx04C-hyGY/edit?usp=sharing) - Analysis of query time for different plans
