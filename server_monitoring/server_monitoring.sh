#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/server_monitoring

kill1=$(ps -eo comm,pid,etimes,cmd | grep server_monitoring.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

cd ${path1}
rm -f ${path1}/*.txt

df -h | tr -d '%' | awk '{if(int($5)>85) print strftime("%Y/%m/%d %H:%M:%S",systime())"|""172.19.4.103""|"$1"|"$2"|"$3"|"$4"|"$5"%|"$6}' >${path1}/usage1.txt

find ${path1}/usage1.txt -type f -size 0 -exec rm -rvf {} \;

if [ -f ${path1}/usage1.txt ]
then
echo "EVENT_DATE|SERVER|FILESYSTEM|SIZE|USED|AVAIL|USE_PCT|MOUNTED" >usage.txt
cat ${path1}/usage1.txt >>usage.txt
fi

if [ -f ${path1}/usage.txt ]
then
java -jar SendMail.jar 2
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
