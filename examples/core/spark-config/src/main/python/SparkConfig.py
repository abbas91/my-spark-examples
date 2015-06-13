import sys
from pyspark import SparkConf
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv) > 2:
    print >> sys.stderr, "Usage: SparkConfig <appName>"
    exit(-1)

  if len(sys.argv) == 2:
    conf = SparkConf().setAppName(sys.argv[1])
    sc = SparkContext(conf=conf)
    print("Application Name: " + sc.appName)
    sc.stop()

  else:
    sc = SparkContext()
    print("Application Name: " + sc.appName)
    sc.stop()
