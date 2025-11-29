# -*- coding: utf-8 -*-
from pyspark import SparkContext

sc = SparkContext(appName="TopRatedMovie")

data = sc.textFile("/user/maria_dev/MovieLens/u.data")
items = sc.textFile("/user/maria_dev/MovieLens/u.item")

ratings = data.map(lambda line: line.split("\t")) \
              .map(lambda parts: (int(parts[1]), 1))

movies = items.map(lambda line: line.split("|")) \
              .map(lambda parts: (int(parts[0]), parts[1]))

rating_counts = ratings.reduceByKey(lambda a, b: a + b)

movie_counts = rating_counts.join(movies)

top_movie = movie_counts.takeOrdered(1, key=lambda x: -x[1][0])[0]

print("Movie ID:", top_movie[0])
print("Title:", top_movie[1][1])
print("Number of Ratings:", top_movie[1][0])
