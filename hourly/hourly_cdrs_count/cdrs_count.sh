#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/hourly/hourly_cdrs_count

kill1=$(ps -eo comm,pid,etimes,cmd | grep cdrs_count.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

cd ${path1}
rm -f ${path1}/*.txt

datepart1=$(date --date="-300 minutes" +%Y%m%d | awk '{print "DATE_"int($0)}')
mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
UPDATE CDRS_TEXT PARTITION("$datepart1") SET ERROR_CODE_ID='9998',ERROR_CODE_DESCRIPTION='REJECTD' WHERE ERROR_CODE_ID='0' AND SMSC_ID='' AND PLATFORM='INFOBIP';
;" 2>/dev/null

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "SELECT LEFT(SUBMIT_DATETIME,13) AS EVENT_DATE_HOUR,SUM(PARTS) AS TOTAL_TRAFFIC,
SUM(CASE WHEN PLATFORM='INFOBIP' THEN PARTS ELSE 0 END) AS INFOBIP,SUM(CASE WHEN PLATFORM='NEXG' THEN PARTS ELSE 0 END) AS NEXG
FROM CDRS_TEXT PARTITION("$datepart1") GROUP BY 1;" 2>/dev/null | sed 's/\t/|/g;' >${path1}/mail.txt

find ${path1}/mail.txt -type f -size 0 -exec rm -rvf {} \;

if [ -f ${path1}/mail.txt ]
then
cat mail.txt | awk -F"|" '{for (i=2;i<=NF;++i) s[i]+=$i;j=NF} END {for(i=2;i<=j;++i) printf "|%s",s[i]; printf "\n";}' | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta' | awk -F"|" '{print "TOTAL""|"$2"|"$3"|"$4}' >>${path1}/mail.txt
java -jar SendMail.jar 2
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
