MovieFameTester
==================

A very basic application to test the popularity of a particular movie based on user ratings. We have a input file called ratings.txt which has user ratings for movies. Data format in file is UserID:MovieID:Rating:Timestamp.This is just a sample file containing 300 rows
Sample Input :- 196:242:3:881250949
This simple application uses the concept of map reduce to calculate the minimum, maximum and average of a particular movie.
Sample Output :- 144--Max=5.0 Min=4.0 Avg=4.25 where 144 is MovieId . This movie has average rating of 4.25 on scale of 5.

The application can be run on large files containing thousands of rows as it uses the concept of map reduce.
