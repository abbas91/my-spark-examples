import sys
from pyspark import SparkContext

if __name__ == "__main__":

  moviesSrc = "movielens/moviegenre"
  genresSrc = "movielens/genre"

  sc = SparkContext()

  movies = sc.textFile(moviesSrc).map(lambda line: line.split('\t')).map(lambda x: (x[1],x[0])).cache()
  print("\n*** (genreId, movieId)")
  for movie in movies.take(5): print '({0},{1})'.format(movie[0],movie[1])

  genres = sc.textFile(genresSrc).map(lambda line: line.split('\t')).map(lambda x: (x[0],x[1])).cache()
  print("\n*** (genreId, genreName)")
  for genre in genres.take(5): print '({0},{1})'.format(genre[0],genre[1])

  results = movies.join(genres).map(lambda (genreId,(movieId,genreName)): (movieId,genreName)).groupByKey()
  print("\n*** (movieId,genreNames)")
  for (movieId,genreNames) in results.take(8): print("(" + movieId + ",(" + ','.join(map(str,genreNames)) + ")")

  print  
  sc.stop()
