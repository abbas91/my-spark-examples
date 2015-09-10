import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
  if len(sys.argv) < 5:
    print "Usage: RequestCountBatch <host> <port> <batchDuration> <filter>"
    exit(0)

  batchDuration = sys.argv[1]

  sc = SparkContext(appName="RequestCountBatch")
  ssc = StreamingContext(sc, Seconds(batchDuration))

  ssc.start()
  ssc.awaitTermination()
  ssc.stop()
