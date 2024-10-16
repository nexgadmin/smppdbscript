#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/ra_reports/user_reco

kill1=$(ps -eo comm,pid,etimes,cmd | grep user_reco.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

cd ${path1}
rm -f ${path1}/*.txt

monthpart1=$(date --date="-1 days" +%Y%m | awk '{print "MONTH_"int($0)}')

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "SELECT USER_NAME,PLATFORM,SUM(DELIVERED) AS CNT FROM DAILY_SUMMARY PARTITION("$monthpart1") A WHERE SUBMITION!=0 AND NOT EXISTS 
(SELECT USER_NAME FROM CRM_RATE_MST B WHERE B.USER_NAME=A.USER_NAME) GROUP BY 1,2 HAVING SUM(DELIVERED)>100 ORDER BY 1 DESC;" 2>/dev/null | sed 's/\t/|/g;' >${path1}/User_List.txt

find ${path1}/User_List.txt -type f -size 0 -exec rm -rvf {} \;

if [ -f ${path1}/User_List.txt ]
then
java -jar SendMail.jar 2
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
