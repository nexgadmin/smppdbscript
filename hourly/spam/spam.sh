#!/bin/bash

currenttime=$(date +%H:%M)

if [[ "$currenttime" > "19:00" ]] || [[ "$currenttime" < "07:30" ]]
then
mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -vvv -e "TRUNCATE TABLE SPAM_DATA;" 2>/dev/null
echo "Spam Exit Time "$currenttime
exit
fi

satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/hourly/spam
export path2=/backup/spam
export path3=/data01/shfiles/hourly/spam/seqn

kill1=$(ps -eo comm,pid,etimes,cmd | grep spam.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

datepart1=$(date --date="-1 hours" +%Y%m%d | awk '{print "DATE_"int($0)}')

min1=$(cat ${path3}/cdrs_seq.txt | head -1)
cnt1=$(mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "SELECT MAX(ID),COUNT(1) FROM CDRS_TEXT PARTITION("$datepart1") WHERE ID >=$min1;" 2>/dev/null | sed 's/\t/|/g;')
cnt2=$(echo $cnt1 | awk -F"|" '{print $2}')
max1=$(echo $cnt1 | awk -F"|" '{if($1=="NULL") print 0} {if($1!="NULL") print $1}')

echo min_seq = $min1 :: max_seq = $max1 :: cdrs_count = $cnt2

if [[ $cnt2 -gt 0 ]]
then

echo $datepart1

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -vvv -e "
INSERT INTO SPAM_DATA (EVENT_DATE,USER_NAME,SENDER_ID,PLATFORM,SUBMITION,DELIVERED,SMS_CONTENT)
SELECT EVENT_DATE,UPPER(CHILD_USER_NAME) AS USER_NAME,UPPER(SENDER_ID) AS SENDER_ID,PLATFORM,SUM(PARTS) AS SUBMITION,
SUM(CASE WHEN ERROR_CODE_ID IN('0','000') THEN PARTS ELSE 0 END) AS DELIVERED,
REGEXP_REPLACE(REPLACE(SMS_CONTENT,SUBSTRING_INDEX(MID(SMS_CONTENT,POSITION('//' IN SMS_CONTENT),100),' ',1),'{}'),'[:digit:]+','{}') AS SMS_CONTENT
FROM CDRS_TEXT WHERE ID BETWEEN $min1 AND $max1
GROUP BY 1,2,3,4,7 ORDER BY 5 DESC;

UPDATE SPAM_DATA A, SPAM_KEYWORDS B SET A.KEYWORD=B.KEYWORD WHERE A.KEYWORD IS NULL AND INSERT_DATE >=(NOW() - INTERVAL 10 MINUTE) AND SMS_CONTENT REGEXP REPLACE(B.KEYWORD,'%','.+');

ALTER TABLE SPAM_FILTER_DATA TRUNCATE PARTITION "$datepart1";

INSERT INTO SPAM_FILTER_DATA (EVENT_DATE,USER_NAME,SENDER_ID,PLATFORM,KEYWORD,SMS_CONTENT,CNT)
SELECT EVENT_DATE,USER_NAME,SENDER_ID,PLATFORM,KEYWORD,REPLACE(REPLACE(SMS_CONTENT,',',' '),'%',' ') AS SMS_CONTENT,SUM(SUBMITION) AS CNT 
FROM SPAM_DATA WHERE KEYWORD IS NOT NULL
GROUP BY 1,2,3,4,5,6;" 2>/dev/null
fi

echo $max1 | awk '{print $0+1}' >${path3}/cdrs_seq.txt

cd ${path1}
rm -f ${path1}/*.txt
rm -f ${path2}/*.csv

mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "SELECT DISTINCT KEYWORD FROM SPAM_FILTER_DATA PARTITION("$datepart1")" 2>/dev/null >${path1}/loop.txt

while read line
do
filename=$(echo $line | sed 's/ /_/g;s/%/_/g;')
echo $filename
mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "SELECT EVENT_DATE,USER_NAME,SENDER_ID,PLATFORM,CNT,KEYWORD,REPLACE(SMS_CONTENT,',',' ') AS SMS_CONTENT FROM SPAM_FILTER_DATA PARTITION("$datepart1") WHERE KEYWORD='"$line"'" 2>/dev/null | sed 's/\t/,/g;' >${path2}/$filename.csv
done < loop.txt

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "SELECT EVENT_DATE,USER_NAME,SENDER_ID,PLATFORM,CNT,KEYWORD,REPLACE(SMS_CONTENT,',',' ') AS SMS_CONTENT FROM SPAM_FILTER_DATA PARTITION("$datepart1")" 2>/dev/null | sed 's/\t/,/g;' >${path2}/SPAM_FILTER_DATA.csv
mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "SELECT EVENT_DATE,USER_NAME,SENDER_ID,PLATFORM,SUBMITION AS CNT,SMS_CONTENT FROM SPAM_DATA WHERE KEYWORD IS NULL AND INSERT_DATE >=(NOW() - INTERVAL 10 MINUTE);" 2>/dev/null | sed 's/\t/,/g;' >${path2}/SMS_CONTENT.csv

export path1=/data01/shfiles/hourly/spam
export path2=/backup/spam
export path3=/data01/shfiles/hourly/spam/seqn

cd ${path1}

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE,PLATFORM,CASE WHEN USER_NAME LIKE '%PSU%' THEN 'PSU' WHEN USER_NAME LIKE '%GOV%' THEN 'GOVT' ELSE 'NORMAL' END AS TYPE,
SUM(SUBMITION) AS SUBMITION,SUM(DELIVERED) AS DELIVERED
FROM SPAM_DATA WHERE EVENT_DATE=DATE(NOW()) GROUP BY 1,2,3;" 2>/dev/null | sed 's/\t/|/g;' >${path1}/hourly_sms_summary.txt

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT DATE(NOW()) AS EVENT_DATE,EMPLOYEE_NAME AS ACCOUNT_MANAGER,CUSTOMER_NAME,EMPLOYEE_EMAILID,CUSTOMER_TYPE_CODE,
IFNULL(SUM(SUBMITION),0) AS SUBMITION,IFNULL(SUM(DELIVERED),0) AS DELIVERED
FROM nexg_hunt.CRM_RATE_MST A
LEFT JOIN (SELECT USER_NAME,SUM(SUBMITION) AS SUBMITION,SUM(DELIVERED) AS DELIVERED
FROM SPAM_DATA WHERE EVENT_DATE=DATE(NOW()) GROUP BY 1) B ON B.USER_NAME=A.USER_NAME
GROUP BY 2,3,4,5 ORDER BY 2,5;" 2>/dev/null | sed 's/\t/|/g;' >${path1}/Account_Manager_wise.txt


if [ -f ${path1}/hourly_sms_summary.txt ]
then
cat hourly_sms_summary.txt | awk -F"|" '{for (i=4;i<=NF;++i) s[i]+=$i;j=NF} END {for(i=4;i<=j;++i) printf "|%s",s[i]; printf "\n";}' | awk -F"|" '{print "TOTAL""|||"$2"|"$3}' >>${path1}/hourly_sms_summary.txt
java -jar SendMail.jar 2
fi

if [ -f ${path1}/Account_Manager_wise.txt ]
then
for i in `cat Account_Manager_wise.txt | sed 's/ /_/g' | awk -F"|" '{split($4,a,"@");if($2!="ACCOUNT_MANAGER") print a[1]"|"$4}' | awk '!x[$0]++' | sort -k 1 -t "|"`
do
ACCOUNT_MANAGER=$(echo $i | awk -F"|" '{print $1}')
EMAIL_ID=$(echo $i | awk -F"|" '{print $2}')
echo $ACCOUNT_MANAGER "::::"$EMAIL_ID 
cat Account_Manager_wise.txt | awk -F"|" '{if($2=="ACCOUNT_MANAGER") print $1"|"$2"|"$3"|"$5"|"$6"|"$7}' >${path1}/mail.txt
cat Account_Manager_wise.txt | awk -F"|" '{if($4=="'"$EMAIL_ID"'") print $1"|"$2"|"$3"|"$5"|"$6"|"$7}' >>${path1}/mail.txt
cat mail.txt | awk -F"|" '{for (i=5;i<=NF;++i) s[i]+=$i;j=NF} END {for(i=5;i<=j;++i) printf "|%s",s[i]; printf "\n";}' | awk -F"|" '{print "TOTAL||||"$2"|"$3}' >>${path1}/mail.txt
java -jar SendMail_Loop.jar $ACCOUNT_MANAGER $EMAIL_ID
done
fi

if [ -f ${path1}/Account_Manager_wise.txt ]
then
cat Account_Manager_wise.txt | awk -F"|" '{ print $1"|"$2"|"$3"|"$5"|"$6"|"$7}' >${path1}/mail.txt 
cat mail.txt | awk -F"|" '{for (i=5;i<=NF;++i) s[i]+=$i;j=NF} END {for(i=5;i<=j;++i) printf "|%s",s[i]; printf "\n";}' | awk -F"|" '{print "TOTAL||||"$2"|"$3}' >>${path1}/mail.txt
java -jar SendMail.jar 3
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""