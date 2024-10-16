#!/bin/bash


python3 /data01/shfiles/file_cute/dlr.py DLR.csv | tr -d '\r' | awk -F"|" '{
    msgdata = $5;  # Use the last column for msgdata
    gsub("%3A", ":", msgdata); 
    stat_value = ""; 
    err_first = ""; 
    err_second = ""; 
    
    # Print msgdata for debugging
    print "Processing msgdata: " msgdata;  
    
    # Extract the "stat" part
    if (match(msgdata, /stat([^+]+)/, stat_match)) {
        stat_value = stat_match[1];  
    } 
    
    # Extract the "err" part and split values before and after "-"
    if (match(msgdata, /err([^-]+)-([^-]+)/, err_match)) {
        err_first = err_match[1]; 
        err_second = err_match[2]; 
    } 
    
    # Print in the desired order
    print $2 "|" $3 "|" stat_value "|" err_first "|" err_second "|" $4 "|" $1;  # Adjusting output order
}' > /data01/shfiles/file_cute/load/DLR_test1.txt

