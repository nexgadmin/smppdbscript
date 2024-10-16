#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/hourly/error_code_summary

kill1=$(ps -eo comm,pid,etimes,cmd | grep error_code_summary.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

cd ${path1}
rm -f ${path1}/*.txt
rm -f ${path1}/*.csv

datepart1=$(date --date="-0 days" +%Y%m%d | awk '{print "DATE_"int($0)}')

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "SELECT PLATFORM,ERROR_CODE_ID,ERROR_CODE_DESCRIPTION,SUM(PARTS) AS CNT 
FROM CDRS_TEXT PARTITION("$datepart1") WHERE ERROR_CODE_ID NOT IN('0','000')
GROUP BY 1,2,3 HAVING COUNT(1) >2000 ORDER BY 4 DESC;" 2>/dev/null | sed 's/\t/|/g;' >${path1}/error_code.txt

find ${path1}/error_code.txt -type f -size 0 -exec rm -rvf {} \;

if [ -f ${path1}/error_code.txt ]
then
cat error_code.txt | tr -d ',' | awk -F"|" '{print $1","$2","$3","$4}'>error_code.csv
java -jar SendMail.jar 3
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
