#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/hourly

kill1=$(ps -eo comm,pid,etimes,cmd | grep hourly.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

sh /data01/shfiles/hourly/error_code_summary/error_code_summary.sh
sh /data01/shfiles/hourly/hourly_cdrs_count/cdrs_count.sh
sh /data01/shfiles/hourly/smsc_id_count/smsc_id_count.sh
sh /data01/shfiles/hourly/spam/spam.sh

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
