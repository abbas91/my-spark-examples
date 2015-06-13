import sys
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv) != 1:
    print "Usage: ColumnStats"
    exit(0)
  sc = SparkContext()
  print "sc.appName =", sc.appName
  sc.stop()
