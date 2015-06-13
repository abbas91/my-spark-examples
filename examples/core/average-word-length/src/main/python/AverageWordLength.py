import sys
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv) != 2:
    print >> sys.stderr, "Usage: AverageWordLength <file or directory>"
    exit(-1)
  
  sc = SparkContext()
  totalWords = sc.accumulator(0)
  totalLetters = sc.accumulator(0.0)
  words = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split())
  def addTotals(word,words,letters):    
    words +=1
    letters += len(word)
  words.foreach(lambda word: addTotals(word,totalWords,totalLetters))
  print "Average word length:", totalLetters.value/totalWords.value
