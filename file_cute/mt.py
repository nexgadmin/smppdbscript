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
    parts = 1
    for row in reader:
        if (int(float(row[8])) == 0):
            content = unquote(row[13].replace('+',' '))
            msg_length = len(content)
            if msg_length > 160:
                parts = math.ceil(msg_length/153)
        if (int(float(row[8])) == 2):
            content = unquote(row[13].replace('+',' '), encoding='utf-16be')
            msg_length = len(content)
            if msg_length > 70:
                parts = math.ceil(msg_length/67)
        print(row[0]+"|"+row[1]+"|"+row[2]+"|"+row[3]+"|"+row[4]+"|"+str(msg_length)+"|"+row[5]+"|"+row[6]+"|"+row[7]+"|"+row[8]+"|"+row[9]+"|"+row[10]+"|"+row[11]+"|"+row[12]+"|"+str(parts)+"|"+content.translate(str.maketrans({'"':'', '|':'', '\\':'', '\n':' '})))