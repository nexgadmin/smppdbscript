import sys
import re
import urllib
import csv
from urllib.parse import unquote
from signal import signal, SIGPIPE, SIG_DFL

# Handling broken pipe errors
signal(SIGPIPE, SIG_DFL)

# Reading the input file
fname = sys.argv[1]
with open(fname, "r") as csvfile:
    reader = csv.reader(csvfile, delimiter='|')
    reason = ""
    status = ""
    
    # Iterate over each row in the CSV file
    for row in reader:
        # Decode the URL-encoded `msgdata` (6th column)
        dlrdata = urllib.parse.unquote(row[5]).replace("+", " ")
        
        # Default values for reason and status
        reason = ""
        status = ""
        
        # Check if 'NACK' is in the data, if so, assign REJECTD status
        if re.search("NACK", dlrdata):
            reason = "9998"
            status = "REJECTD"
        
        else:
            # Split the data and search for `stat:` and `err:` patterns
            for m in dlrdata.split():
                if re.search("stat:", str(m)):
                    # Extract status
                    status = m.split(":")[1]
                    
                if re.search("err:", str(m)):
                    # Extract error values
                    reason = m.split(":")[1]
                    # Split the `err` value into two parts before and after `-`
                    err_parts = reason.split("-")
                    if len(err_parts) == 2:
                        reason = err_parts[0]  # First part of `err`
                        err_second = err_parts[1]  # Second part of `err`
                    else:
                        reason = err_parts[0]
                        err_second = ""  # If there's no second part
        
        # Printing the formatted output (with desired columns)
        print(f"{row[1]}|{row[2]}|{status}|{reason}|{err_second}|{row[3]}|{row[4]}")

