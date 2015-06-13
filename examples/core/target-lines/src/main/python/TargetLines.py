import sys
from pyspark import SparkContext

if __name__ == "__main__":

  if len(sys.argv) != 3:
    print >> sys.stderr, "Usage: TargetLines <path> <list>"
    exit(-1)

  sc = SparkContext()
  listbc = sc.broadcast(sys.argv[2].split(" "))
  targetLines = sc.textFile(sys.argv[1]).filter(lambda line: any(v in line for v in listbc.value))
  for line in targetLines.collect(): print(line)
  sc.stop()
