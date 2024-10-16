#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/ra_reports/account_manager_report

kill1=$(ps -eo comm,pid,etimes,cmd | grep account_manager.sh | awk '{if($3 > 1800) print $2}')

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

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT DISTINCT EMPLOYEE_NAME,CUSTOMER_NAME,EMPLOYEE_EMAILID,CUSTOMER_TYPE_CODE,IFNULL(SUM(TOTAL_SUBMITTED),0) AS TOTAL_SUBMITTED,IFNULL(SUM(TOTAL_DELIVERED),
0) AS TOTAL_DELIVERED,CONCAT(IFNULL(ROUND((SUM(TOTAL_DELIVERED)/SUM(TOTAL_SUBMITTED)) * 100,0),0),'%') AS TOTAL_DELIVERED_PCT,
IFNULL(SUM(YESTERDAY_SUBMITTED),0) AS YESTERDAY_SUBMITTED,IFNULL(SUM(YESTERDAY_DELIVERED),0) AS YESTERDAY_DELIVERED,
CONCAT(IFNULL(ROUND((SUM(YESTERDAY_DELIVERED)/SUM(YESTERDAY_SUBMITTED)) * 100,0),0),'%') AS YESTERDAY_DELIVERED_PCT
FROM (SELECT USER_NAME,SUM(SUBMITION) AS TOTAL_SUBMITTED,SUM(DELIVERED) AS TOTAL_DELIVERED
,SUM(CASE WHEN EVENT_DATE=SUBDATE(CURDATE(),1) THEN SUBMITION ELSE 0 END) AS YESTERDAY_SUBMITTED
,SUM(CASE WHEN EVENT_DATE=SUBDATE(CURDATE(),1) THEN DELIVERED ELSE 0 END) AS YESTERDAY_DELIVERED
FROM DAILY_SUMMARY PARTITION("$monthpart1") GROUP BY 1) A
RIGHT JOIN (SELECT DISTINCT USER_NAME,CUSTOMER_NAME,EMPLOYEE_NAME,EMPLOYEE_EMAILID,CUSTOMER_TYPE_CODE FROM CRM_RATE_MST WHERE USER_NAME!='0' AND SERVICE_NAME='Bulk SMS') B ON B.USER_NAME=A.USER_NAME
GROUP BY 1,2,3 ORDER BY 1,2;" 2>/dev/null | sed 's/\t/|/g;' >${path1}/sms_summary.txt

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT DISTINCT IFNULL(EMPLOYEE_NAME,'NOT_FOUND') AS EMPLOYEE_NAME,COUNT(DISTINCT IFNULL(CUSTOMER_CODE,A.USER_NAME)) AS NUMBER_OF_ACCOUNTS,
IFNULL(SUM(TOTAL_SUBMITTED),0) AS TOTAL_SUBMITTED,IFNULL(SUM(TOTAL_DELIVERED),0) AS TOTAL_DELIVERED,
CONCAT(IFNULL(ROUND((SUM(TOTAL_DELIVERED)/SUM(TOTAL_SUBMITTED)) * 100,0),0),'%') AS TOTAL_DELIVERED_PCT,
IFNULL(SUM(YESTERDAY_SUBMITTED),0) AS YESTERDAY_SUBMITTED,IFNULL(SUM(YESTERDAY_DELIVERED),0) AS YESTERDAY_DELIVERED,
CONCAT(IFNULL(ROUND((SUM(YESTERDAY_DELIVERED)/SUM(YESTERDAY_SUBMITTED)) * 100,0),0),'%') AS YESTERDAY_DELIVERED_PCT
FROM (SELECT USER_NAME,SUM(SUBMITION) AS TOTAL_SUBMITTED,SUM(DELIVERED) AS TOTAL_DELIVERED
,SUM(CASE WHEN EVENT_DATE=SUBDATE(CURDATE(),1) THEN SUBMITION ELSE 0 END) AS YESTERDAY_SUBMITTED
,SUM(CASE WHEN EVENT_DATE=SUBDATE(CURDATE(),1) THEN DELIVERED ELSE 0 END) AS YESTERDAY_DELIVERED
FROM DAILY_SUMMARY PARTITION("$monthpart1") GROUP BY 1) A
RIGHT JOIN (SELECT DISTINCT USER_NAME,EMPLOYEE_NAME,CUSTOMER_CODE FROM CRM_RATE_MST WHERE USER_NAME!='0' AND SERVICE_NAME='Bulk SMS') B ON B.USER_NAME=A.USER_NAME
GROUP BY 1 ORDER BY 1;" 2>/dev/null | sed 's/\t/|/g;' >${path1}/sms_summary1.txt

cat sms_summary1.txt | awk -F"|" '{for (i=2;i<=NF;++i) s[i]+=$i;j=NF} END {for(i=2;i<=j;++i) printf "|%s",s[i]; printf "\n";}' | awk -F "|" '{print "TOTAL""|"$2"|"$3"|"$4"|"int($4/$3*100)"%|"$6"|"$7"|"int($7/$6*100)"%"}' >>${path1}/sms_summary1.txt

find ${path1}/sms_summary.txt -type f -size 0 -exec rm -rvf {} \;

if [ -f ${path1}/sms_summary.txt ]
then
for i in `cat sms_summary.txt | sed 's/ /_/g' | awk -F"|" '{split($3,a,"@");if($1!="EMPLOYEE_NAME") print a[1]"|"$3}' | awk '!x[$0]++' | sort -k 1 -t "|"`
do
ACCOUNT_MANAGER=$(echo $i | awk -F"|" '{print $1}')
EMAIL_ID=$(echo $i | awk -F"|" '{print $2}')
echo $ACCOUNT_MANAGER "::::"$EMAIL_ID 
cat sms_summary.txt | awk -F"|" '{if($1=="EMPLOYEE_NAME") print $2"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10}' >${path1}/mail.txt
cat sms_summary.txt | awk -F"|" '{if($3=="'"$EMAIL_ID"'") print $2"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10}' >>${path1}/mail.txt
cat mail.txt | awk -F"|" '{for (i=3;i<=NF;++i) s[i]+=$i;j=NF} END {for(i=3;i<=j;++i) printf "|%s",s[i]; printf "\n";}' | awk -F"|" '{print "TOTAL""||"$2"|"$3"|"int($3/$2*100)"%|"$5"|"$6"|"int($6/$5*100)"%"}' >>${path1}/mail.txt
java -jar SendMail_Loop.jar $ACCOUNT_MANAGER $EMAIL_ID
done
fi

if [ -f ${path1}/sms_summary.txt ]
then
cat sms_summary.txt | awk -F"|" '{if($2=="CUSTOMER_NAME") print $2"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10}' >${path1}/mail1.txt
cat sms_summary.txt | awk -F"|" '{if($2!="CUSTOMER_NAME") print $2"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10}' | sort -k3,3nr -t "|" >>${path1}/mail1.txt
cat mail1.txt | awk -F"|" '{for (i=3;i<=NF;++i) s[i]+=$i;j=NF} END {for(i=3;i<=j;++i) printf "|%s",s[i]; printf "\n";}' | awk -F"|" '{print "TOTAL""||"$2"|"$3"|"int($3/$2*100)"%|"$5"|"$6"|"int($6/$5*100)"%"}' >>${path1}/mail1.txt
java -jar SendMail.jar 2
fi

if [ -f ${path1}/sms_summary1.txt ]
then
sed -i '49d' ${path1}/conf/loader.properties
cat sms_summary.txt | awk -F"|" '{ print $3}' | grep "@" | awk '!x[$0]++' | xargs | sed 's/ /,/g' | awk '{print "3_TO="$0}'>>${path1}/conf/loader.properties
java -jar SendMail.jar 3
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
