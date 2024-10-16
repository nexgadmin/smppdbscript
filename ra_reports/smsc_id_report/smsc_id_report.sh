#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/ra_reports/smsc_id_report

kill1=$(ps -eo comm,pid,etimes,cmd | grep smsc_id_report.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

cd ${path1}
rm -f ${path1}/*.txt

datepart1=$(date --date="-1 days" +%Y%m%d | awk '{print "DATE_"int($0)}')

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE,PLATFORM,SMSC_ID,SMSC_NAME,SUM(SUBMITION) AS SUBMITION,SUM(DELIVERED) AS DELIVERED,SUM(FAILED) AS FAILED
FROM BILLING_SUMMARY PARTITION("$datepart1") GROUP BY 1,2,3,4;" 2>/dev/null | sed 's/\t/|/g;' >${path1}/mail.txt

find ${path1}/mail.txt -type f -size 0 -exec rm -rvf {} \;

if [ -f ${path1}/mail.txt ]
then
cat mail.txt | awk -F"|" '{for (i=5;i<=NF;++i) s[i]+=$i;j=NF} END {for(i=5;i<=j;++i) printf "|%s",s[i]; printf "\n";}' | awk -F"|" '{print "TOTAL""||||"$2"|"$3"|"$4}' >>${path1}/mail.txt
java -jar SendMail.jar 2
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
