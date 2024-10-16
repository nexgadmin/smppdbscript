#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/ra_reports/client_summary

kill1=$(ps -eo comm,pid,etimes,cmd | grep client.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

cd ${path1}
rm -f ${path1}/*.txt

monthpart1=$(date --date="-2 days" +%Y-%m- | awk '{print $0"01"}')

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE,CUSTOMER_NAME,A.USER_NAME,TOTAL_SUBMIT,DELIVERED,DELIVERED_PCT,FAILED,FAILED_PCT,EMPLOYEE_EMAILID,CUSTOMER_EMAIL_ID FROM
(SELECT PLATFORM,EVENT_DATE,USER_NAME,SUM(SUBMITION) AS TOTAL_SUBMIT,SUM(DELIVERED) AS DELIVERED,ROUND((SUM(DELIVERED)/SUM(SUBMITION))*100) AS DELIVERED_PCT
,FAILED,ROUND((SUM(FAILED)/SUM(SUBMITION))*100) AS FAILED_PCT
FROM sms_cdrs.DAILY_SUMMARY WHERE EVENT_DATE >='"$monthpart1"' GROUP BY 1,2,3) A, (SELECT DISTINCT USER_NAME,CUSTOMER_NAME,EMPLOYEE_EMAILID,CUSTOMER_EMAIL_ID FROM nexg_hunt.CRM_RATE_MST
WHERE SERVICE_NAME='Bulk SMS' AND SUMMARY_EMAIL='YES') B
WHERE A.USER_NAME=B.USER_NAME AND TOTAL_SUBMIT>0 ORDER BY 1 DESC;" 2>/dev/null | sed 's/\t/|/g;' >${path1}/summary.txt

mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE,CUSTOMER_NAME,A.USER_NAME,SUM(SUBMITION) AS TOTAL_SUBMIT,SUM(DELIVERED) AS DELIVERED
,ROUND((SUM(DELIVERED)/SUM(SUBMITION))*100) AS DELIVERED_PCT,FAILED
,ROUND((SUM(FAILED)/SUM(SUBMITION))*100) AS FAILED_PCT
,CC_ID AS EMPLOYEE_EMAILID,TO_ID AS CUSTOMER_EMAIL_ID FROM sms_cdrs.DAILY_SUMMARY A, nexg_hunt.MAIL_ALERT B
WHERE A.EVENT_DATE >='"$monthpart1"' AND A.USER_NAME=B.USER_NAME AND SUBMITION>0 
GROUP BY 1,2,3,9,10 ORDER BY 1 DESC;" 2>/dev/null | sed 's/\t/|/g;' >>${path1}/summary.txt

find ${path1}/*.txt -type f -size 0 -exec rm -rvf {} \;

if [ -f ${path1}/summary.txt ]
then
for i in `cat summary.txt | sed 's/ /_/g' | awk -F"|" '{if($1!="EVENT_DATE") print $9"|"$10"|"$2}' | awk '!x[$0]++' | sort -k 1 -t "|"`
do
ACCOUNT_MANAGER_ID=$(echo $i | awk -F"|" '{print $1}')
CLIENT_ID=$(echo $i | awk -F"|" '{print $2}')
CLIENT_NAME=$(echo $i | awk -F"|" '{print $3}')
echo $ACCOUNT_MANAGER_ID"::::"$CLIENT_ID"::::"$CLIENT_NAME

cat summary.txt | awk -F"|" '{if($1=="EVENT_DATE") print $1"|"$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8}' >${path1}/mail.txt
cat summary.txt | awk -F"|" '{if($10=="'"$CLIENT_ID"'") print $1"|"$2"|"$3"|"$4"|"$5"|"$6"%|"$7"|"$8"%"}' >>${path1}/mail.txt
cat summary.txt | awk -F"|" '{if($10=="'"$CLIENT_ID"'") print $1"|"$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8}' | awk -F"|" '{for (i=4;i<=NF;++i) s[i]+=$i;j=NF} END {for(i=4;i<=j;++i) printf "|%s",s[i]; printf "\n";}' | awk -F"|" '{print "TOTAL""|||"$2"|"$3"|"int($3/$2*100)"%|"$5"|"int($5/$2*100)"%"}' >>${path1}/mail.txt
sed -i '14d;15d;16d;17d' ${path1}/conf/loader.properties
echo "2_DISPLAYNAME=Report For "$CLIENT_NAME >>${path1}/conf/loader.properties
echo "2_REPTO="$ACCOUNT_MANAGER_ID >>${path1}/conf/loader.properties
echo "2_TO="$CLIENT_ID >>${path1}/conf/loader.properties
echo "2_CC="$ACCOUNT_MANAGER_ID",baljeet.singh@nexgplatforms.com" >>${path1}/conf/loader.properties
java -jar SendMail.jar 2
done
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""

