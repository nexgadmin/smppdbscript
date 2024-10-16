#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/ra_reports/sms_summary

kill1=$(ps -eo comm,pid,etimes,cmd | grep sms_summary.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

cd ${path1}
rm -f ${path1}/*.txt

month1=$(date --date="-1 days" +%Y%m | awk '{print "MONTH_"int($0)}')
echo $month1

SUBMITION=$(mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SET SESSION group_concat_max_len=4294967295;
SELECT GROUP_CONCAT(DISTINCT CONCAT('SUM(CASE WHEN EVENT_DATE=''',DT,''' THEN SUBMITION ELSE 0 END) AS DAY_',DAY(DT)) ORDER BY 1 DESC)
FROM (SELECT DISTINCT EVENT_DATE AS DT FROM DAILY_SUMMARY PARTITION("$month1") ORDER BY 1) A;" 2>/dev/null)

DELIVERED=$(mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SET SESSION group_concat_max_len=4294967295;
SELECT GROUP_CONCAT(DISTINCT CONCAT('SUM(CASE WHEN EVENT_DATE=''',DT,''' THEN DELIVERED ELSE 0 END) AS DAY_',DAY(DT)) ORDER BY 1 DESC)
FROM (SELECT DISTINCT EVENT_DATE AS DT FROM DAILY_SUMMARY PARTITION("$month1") ORDER BY 1) A;" 2>/dev/null)

FAILED=$(mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SET SESSION group_concat_max_len=4294967295;
SELECT GROUP_CONCAT(DISTINCT CONCAT('SUM(CASE WHEN EVENT_DATE=''',DT,''' THEN FAILED ELSE 0 END) AS DAY_',DAY(DT)) ORDER BY 1 DESC)
FROM (SELECT DISTINCT EVENT_DATE AS DT FROM DAILY_SUMMARY PARTITION("$month1") ORDER BY 1) A;" 2>/dev/null)


mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT PLATFORM,'SUBMITION' AS STATUS,SUM(SUBMITION) AS MONTH_TILL_DATE,""$SUBMITION"" FROM DAILY_SUMMARY PARTITION("$month1") WHERE PLATFORM='6D' GROUP BY 1,2
UNION ALL
SELECT PLATFORM,'DELIVERED' AS STATUS,SUM(DELIVERED) AS MONTH_TILL_DATE,""$DELIVERED"" FROM DAILY_SUMMARY PARTITION("$month1") WHERE PLATFORM='6D' GROUP BY 1,2
UNION ALL
SELECT PLATFORM,'FAILED' AS STATUS,SUM(SUBMITION - DELIVERED) AS MONTH_TILL_DATE,""$FAILED"" FROM DAILY_SUMMARY PARTITION("$month1") WHERE PLATFORM='6D' GROUP BY 1,2
UNION ALL
SELECT PLATFORM,'SUBMITION' AS STATUS,SUM(SUBMITION) AS MONTH_TILL_DATE,""$SUBMITION"" FROM DAILY_SUMMARY PARTITION("$month1") WHERE PLATFORM='INFOBIP' GROUP BY 1,2
UNION ALL
SELECT PLATFORM,'DELIVERED' AS STATUS,SUM(DELIVERED) AS MONTH_TILL_DATE,""$DELIVERED"" FROM DAILY_SUMMARY PARTITION("$month1") WHERE PLATFORM='INFOBIP' GROUP BY 1,2
UNION ALL
SELECT PLATFORM,'FAILED' AS STATUS,SUM(SUBMITION - DELIVERED) AS MONTH_TILL_DATE,""$FAILED"" FROM DAILY_SUMMARY PARTITION("$month1") WHERE PLATFORM='INFOBIP' GROUP BY 1,2
UNION ALL
SELECT PLATFORM,'SUBMITION' AS STATUS,SUM(SUBMITION) AS MONTH_TILL_DATE,""$SUBMITION"" FROM DAILY_SUMMARY PARTITION("$month1") WHERE PLATFORM='NEXG' GROUP BY 1,2
UNION ALL
SELECT PLATFORM,'DELIVERED' AS STATUS,SUM(DELIVERED) AS MONTH_TILL_DATE,""$DELIVERED"" FROM DAILY_SUMMARY PARTITION("$month1") WHERE PLATFORM='NEXG' GROUP BY 1,2
UNION ALL
SELECT PLATFORM,'FAILED' AS STATUS,SUM(SUBMITION - DELIVERED) AS MONTH_TILL_DATE,""$FAILED"" FROM DAILY_SUMMARY PARTITION("$month1") WHERE PLATFORM='NEXG' GROUP BY 1,2;
" 2>/dev/null | sed 's/\t/|/g;' >${path1}/mail.txt

date1=$(date --date="-1 days" +%Y-%m | awk '{print $0"-01"}')
mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE,SMSC_NAME,CASE WHEN USER_NAME LIKE '%PSU%' THEN 'PSU' WHEN USER_NAME LIKE '%GOV%' THEN 'GOVT' ELSE 'NORMAL' END AS SERVICE_TYPE,
SUM(SUBMITION) AS SUBMITION,SUM(DELIVERED) AS DELIVERED
FROM BILLING_SUMMARY WHERE EVENT_DATE>='"$date1"' GROUP BY 1,2,3 ORDER BY 1 DESC;
" 2>/dev/null | sed 's/\t/|/g;' >${path1}/sms_summary_Operator_wise_MTD.txt

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE,PLATFORM,CASE WHEN USER_NAME LIKE '%PSU%' THEN 'PSU' WHEN USER_NAME LIKE '%GOV%' THEN 'GOVT' ELSE 'NORMAL' END AS TYPE,
SUM(SUBMITION) AS SUBMITION,SUM(DELIVERED) AS DELIVERED
 from DAILY_SUMMARY partition("$month1") GROUP BY 1,2,3 ORDER BY 1 DESC;
" 2>/dev/null | sed 's/\t/|/g;' >${path1}/sms_summary_Platform_wise_MTD.txt

find ${path1}/*.txt -type f -size 0 -exec rm -rvf {} \;

sed -i '49d;50d' ${path1}/conf/loader.properties

cat mail.txt | head -1 | awk '{print "2_HEADER1="$0}' | sed 's/|/<\/th><th>/g;' >>${path1}/conf/loader.properties
cat mail.txt | head -1 | awk -F"|" '{print "2_COLSPAN="NF}' >>${path1}/conf/loader.properties

# val1=$(cat mail.txt | head -1 | awk -F"|" '{print "<tr bgcolor='#CAE1FF'><th colspan="NF">INFOBIP STATUS WISE COUNT SUMMARY</th></tr>"}')
# sed -i '5 i '"$val1"'' mail.txt

if [ -f ${path1}/mail.txt ]
then
java -jar SendMail.jar 2
fi

if [ -f ${path1}/sms_summary_Operator_wise_MTD.txt ]
then
cat sms_summary_Operator_wise_MTD.txt | awk -F"|" '{for (i=4;i<=NF;++i) s[i]+=$i;j=NF} END {for(i=4;i<=j;++i) printf "|%s",s[i]; printf "\n";}' | awk -F"|" '{print "TOTAL""|||"$2"|"$3}' >>${path1}/sms_summary_Operator_wise_MTD.txt
java -jar SendMail.jar 3
fi

if [ -f ${path1}/sms_summary_Platform_wise_MTD.txt ]
then
cat sms_summary_Platform_wise_MTD.txt | awk -F"|" '{for (i=4;i<=NF;++i) s[i]+=$i;j=NF} END {for(i=4;i<=j;++i) printf "|%s",s[i]; printf "\n";}' | awk -F"|" '{print "TOTAL""|||"$2"|"$3}' >>${path1}/sms_summary_Platform_wise_MTD.txt
java -jar SendMail.jar 4
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
