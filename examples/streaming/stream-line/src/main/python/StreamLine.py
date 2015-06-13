import sys
import time
import socket

if __name__ == "__main__":
  if len(sys.argv) < 4:
    print >> sys.stderr, "Usage: StreamLine <host> <port> <lines-per-second> <filepath>"
    exit(-1)

  host = sys.argv[1]
  port = int(sys.argv[2])
  duration = 1/float(sys.argv[3])
  filelist = sys.argv[4]

  try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  except:
    print "Failed to open socket."
    sys.exit(1)

  try:
    s.bind((host,port))
    s.listen(1)

    print "Waiting for connection on", host, ":", port
    conn, addr = s.accept()
    print "Connected by", addr
    
    for filename in filelist:
      with open(filelist) as f:
        for line in f: 
          print line
          conn.send(line)
          time.sleep(duration)

  except IOError as e:
    print e.strerror + "."
    
  except:
    print "\nUnexpected termination."

  finally:
    s.close()

