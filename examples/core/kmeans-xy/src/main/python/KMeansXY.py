import sys
import operator
from pyspark import SparkContext

if __name__ == "__main__":

  def distanceSquared(p1,p2):  
    return (p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2

  def addPoints(p1,p2):
    return [p1[0] + p2[0], p1[1] + p2[1]]

  def closestPoint(p, points):
    index = 0
    closest = float("+inf")
    for i in range(len(points)):
      dist = distanceSquared(p,points[i])
      if dist < closest:
        closest = dist
        index = i
    return index

  coordinates = [
    (1,5),(3,4),(7,9),(2,2),(6,2),(12,3),(9,12),
    (4,6),(7,4),(9,11),(9,7),(10,10),(11,10),(2,3),
    (1,3),(10,1),(11,2),(10,3),(9,1),(9,3),(10,4),
    (2,9),(1,8),(2,5),(10,11),(2,7),(10,2)]
      
  K = 5
  convergeDist = .1
  
  sc = SparkContext()

  points = sc.parallelize(coordinates).map(lambda (x,y): (float(x), float(y))).cache()
  kPoints = [(1.0,1.0),(12.0,1.0),(12.0,12.0)]

  print "\nStarting K points:" 
  for kPoint in kPoints: print kPoint
  print

  tempDist = float("+inf")
  while tempDist > convergeDist:
    closest = points.map(lambda p : (closestPoint(p, kPoints), (p, 1)))
    pointStats = closest.reduceByKey(lambda (point1,n1),(point2,n2):  (addPoints(point1,point2),n1+n2) )
    newPoints = pointStats.map(lambda (i,(point,n)): (i,[point[0]/n,point[1]/n])).collect()
    tempDist=0
    for  (i,point) in newPoints: tempDist += distanceSquared(kPoints[i],point)
    print "Distance between iterations:",tempDist
    for (i, point) in newPoints: kPoints[i] = point
          
  print "\nFinal K points:"
  for kPoint in kPoints: print kPoint
  print
  sc.stop()

