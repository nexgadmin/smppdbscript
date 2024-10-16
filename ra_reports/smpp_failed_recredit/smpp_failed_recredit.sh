#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/ra_reports/smpp_failed_recredit

kill1=$(ps -eo comm,pid,etimes,cmd | grep smpp_failed_recredit.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

cd ${path1}
rm -f ${path1}/*.txt

date1=$(date --date="-1 days" +%Y-%m-%d)
echo "EVENT_DATE|CUSTOMER_NAME|USER_NAME|CUSTOMER_TYPE|SERVICE_TYPE|SUBMITION|DELIVERED|FAILED|RE_CREDIT_CNT|BEFORE_CREDIT_BAL|AFTER_CREDIT_BAL" >${path1}/mail1.txt

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh test -e "
SELECT A.EVENT_DATE,REPLACE(B.CUSTOMER_NAME,' ','_') AS CUSTOMER_NAME,LOWER(A.USER_NAME) AS USER_NAME,B.CUSTOMER_TYPE_CODE,B.SERVICE_TYPE,A.SUBMITION,A.DELIVERED,A.FAILED,
FLOOR((ROUND(A.FAILED * B.CUSTOMER_BASE_RATE,2)) / (B.CUSTOMER_BASE_RATE + B.DLT_RATE)) AS CREDIT_BACK_CNT,
B.CUSTOMER_EMAIL_ID,B.EMPLOYEE_EMAILID
FROM sms_cdrs.DAILY_SUMMARY A, nexg_hunt.CRM_RATE_MST B 
WHERE A.USER_NAME=B.USER_NAME AND B.CUSTOMER_TYPE_CODE='PRE' AND B.CHARGE_TYPE='ODLV' AND A.FAILED >1 AND A.PLATFORM='NEXG'
AND A.EVENT_DATE>='$date1' ORDER BY 3,1;" 2>/dev/null | sed 's/\t/|/g;' >${path1}/data.txt

for i in $(cat data.txt | awk -F"|" '{if($1!="EVENT_DATE") print $0}')
do
echo "EVENT_DATE|CUSTOMER_NAME|USER_NAME|CUSTOMER_TYPE|SERVICE_TYPE|SUBMITION|DELIVERED|FAILED|RE_CREDIT_CNT|BEFORE_CREDIT_BAL|AFTER_CREDIT_BAL" >${path1}/mail.txt
echo $i
user_name=$(echo $i | awk -F"|" '{print $3":credit"}')
user_name1=$(echo $i | awk -F"|" '{print $3}')
failed_cnt=$(echo $i | awk -F"|" '{print $9}')
CLIENT_ID=$(echo $i | awk -F"|" '{print $10}')
ACCOUNT_MANAGER_ID=$(echo $i | awk -F"|" '{print $11}')
CLIENT_NAME=$(echo $i | awk -F"|" '{split($10,a,"@"); print a[1]}')
echo $user_name"::::"$failed_cnt"::::"$CLIENT_ID"::::"$ACCOUNT_MANAGER_ID"::::"$CLIENT_NAME

redis_credit=$(redis-cli -h 172.19.8.158 -p 6370 -a 'Vp$9~ioKy6t^8&e' hincrby smpp_user $user_name $failed_cnt)
echo $redis_credit
echo $i | awk -F"|" '{print $1"|"$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"'$redis_credit' - '$failed_cnt'"|"'$redis_credit'}' >>${path1}/mail.txt
echo $i | awk -F"|" '{print $1"|"$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"'$redis_credit' - '$failed_cnt'"|"'$redis_credit'}' >>${path1}/mail1.txt

mysql -h172.19.8.164 -uroot -p'Vp$9~ioKy6t^8&e' nexgreach -vvv -e "INSERT INTO credit_history(user_id,recredit,balance,channel,username) SELECT user_id,$failed_cnt,$redis_credit,'smpp','$user_name1' FROM usersmppcredentials WHERE LOWER(smppusername)='$user_name1' limit 1" 2>/dev/null

find ${path1}/mail.txt -type f -size 0 -exec rm -rvf {} \;

if [ -f ${path1}/mail.txt ]
then
sed -i '32d;33d;34d;35d' ${path1}/conf/loader.properties
echo "2_DISPLAYNAME=Re Credit For "$CLIENT_NAME >>${path1}/conf/loader.properties
echo "2_REPTO="$ACCOUNT_MANAGER_ID >>${path1}/conf/loader.properties
echo "2_TO="$CLIENT_ID >>${path1}/conf/loader.properties
echo "2_CC="$ACCOUNT_MANAGER_ID",tarun.sharma@nexgplatforms.com" >>${path1}/conf/loader.properties
# java -jar SendMail.jar 2
fi
done

find ${path1}/mail1.txt -type f -size 0 -exec rm -rvf {} \;

if [ -f ${path1}/mail1.txt ]
then
java -jar SendMail.jar 1
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
