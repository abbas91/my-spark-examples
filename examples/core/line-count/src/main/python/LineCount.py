import sys
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv) < 2 or len(sys.argv) > 3:
    print >> sys.stderr, "Usage: LineCount <path> <search>"
    exit(-1)
  path = sys.argv[1]
  count = 0
  sc = SparkContext()
  if len(sys.argv) == 2:
    count = sc.textFile(path).count()
  else:
    search = sys.argv[2]
    count = sc.textFile(path).filter(lambda line: search in line).count()
  print "Line Count:", count
  sc.stop()
