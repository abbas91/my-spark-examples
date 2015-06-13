import re
import sys
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv) != 2:
    print "Usage: WordCount <file or directory>"
    exit(0)
  
  sc = SparkContext()
  pairs = (sc.textFile(sys.argv[1])
    .flatMap(lambda line: re.split("[. ;:,]+", line))
    .map(lambda word: (word,1))
    .filter(lambda (word, length): word)
    .reduceByKey(lambda v1,v2: v1+v2)
    .map(lambda (word,count): (count,word))
    .sortByKey(False)
    .map(lambda (count,word): (word,count)))
  for pair in pairs.take(5):
    print '{0}, {1}'.format(pair[0], pair[1])
  sc.stop()
