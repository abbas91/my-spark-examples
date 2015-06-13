import sys
from pyspark import SparkContext

if __name__ == "__main__":

  def computeContribs(neighbors, rank):
    for neighbor in neighbors: yield(neighbor, rank/len(neighbors))

  sc = SparkContext()

  data = ["page1 page3","page2 page1","page4 page1","page3 page1","page4 page2","page3 page4"]

  links = sc.parallelize(data).map(lambda line: line.split()).map(lambda pages: (pages[0],pages[1])).distinct().groupByKey().cache()
  print("\n(Source, (Target List))")
  for link in links.collect(): print '({0}, ({1}))'.format(link[0], ', '.join(link[1]))

  ranks = links.map(lambda (page,neighbors): (page,1.0))
  print("\n(Page, InitialRank)")
  for rank in ranks.collect(): print '({0}, {1})'.format(rank[0], rank[1])

  for x in xrange(10):
    contribs = links.join(ranks).flatMap(lambda (page,(neighbors,rank)): computeContribs(neighbors,rank))
    ranks = contribs.reduceByKey(lambda v1,v2: v1+v2).map(lambda (page,contrib): (page,contrib * 0.85 + 0.15))
    print "\nIteration ", x+1
    for pair in ranks.take(10): print '({0}, {1})'.format(pair[0], pair[1])
  print
