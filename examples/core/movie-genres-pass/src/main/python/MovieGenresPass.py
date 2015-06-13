import sys
from pyspark import SparkContext

if __name__ == "__main__":

  moviesSrc = "movielens/moviegenre"
  genresSrc = "/tmp/genre.txt"

  sc = SparkContext()

  movies = sc.textFile(moviesSrc).map(lambda line: line.split('\t')).map(lambda x: (x[1],x[0])).cache()
  print("\n*** (genreId, movieId)")
  for movie in movies.take(5): print '({0},{1})'.format(movie[0],movie[1])

  genres = dict(map(lambda tokens:(tokens[0],tokens[1]), map(lambda line: line.rstrip('\n').split('\t'), open(genresSrc))))
  print("\n*** (genreId, genreName)")
  for genre in genres.items()[0:5]: print '({0},{1})'.format(genre[0],genre[1])

  results = movies.map(lambda (genreId,movieId):(movieId, genres[genreId])).groupByKey()
  print("\n*** (movieId, genreNames)")
  for (movieId,genreNames) in results.take(8): print("(" + movieId + ",(" + ','.join(map(str,genreNames)) + ")")

  print  
  sc.stop()
