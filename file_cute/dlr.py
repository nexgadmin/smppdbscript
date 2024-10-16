import sys
import math
import re
import urllib
import csv
from urllib.parse import parse_qs
from urllib.parse import urlparse
from urllib.parse import unquote
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)
# print("\nName of Python script:", sys.argv[1])
fname = sys.argv[1]
with open(fname, "r") as csvfile:
    reader = csv.reader(csvfile, delimiter='|')
    reason = None
    status = ""
    for row in reader:
        dlrdata = urllib.parse.unquote(row[5]).replace("+"," ")
        if re.search("NACK", dlrdata):
            reason = "9998"
            status = "REJECTD"
            print(row[0]+"|"+row[1]+"|"+row[2]+"|"+row[3]+"|"+row[4]+"|"+reason+"|"+status)
        else:
            for m in dlrdata.split():
                if (re.search("stat:", str(m))):
                    (key, val) = m.split(":")
                    status = val
                if (re.search("err:", str(m))):
                    A = None
                    (key, val) = m.split(":")
                    reason = val
                    for x in reason.split("-"):
                        A = x
                    print(row[0]+"|"+row[1]+"|"+row[2]+"|"+row[3]+"|"+row[4]+"|"+A+"|"+status)
