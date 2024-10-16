#!/bin/sh
path1=/data01/shfiles/materialized_view/view_log
echo "1" >${path1}/lock.log

cd /data01/shfiles/materialized_view/cdrs/infobip/cdrs_view

for i in `ls -1 | grep ".*\.csv$"`
do
cat $i | awk -F"|" '{ print $2"|"$12"|"$13"|"$4"|"$5"|"$7"|"$8"|"$10"|"$18"|"$14"|"$15"|"$16"|"$17"|"$23"|"$24"|"$27"|"$21"|"$22"|INFOBIP|"$29"|"$1}' | sed 's/\\/#/g' | tr -d '"' >/data01/shfiles/materialized_view/cdrs/cdrs_view/$i.tmp
mv /data01/shfiles/materialized_view/cdrs/cdrs_view/$i.tmp /data01/shfiles/materialized_view/cdrs/cdrs_view/$i
rm -f $i
done

cd /data01/shfiles/materialized_view/cdrs/cdrs_view

for i in `ls -1 | grep ".*\.csv$"`
do
echo $i
mysql -h172.19.4.103 -uroot -p'p*E9@r#Xnh' sms_cdrs --local-infile -vvv -w -e "LOAD DATA LOCAL INFILE '$i' INTO TABLE CDRS_TEXT
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
(EVENT_DATE,SUBMIT_DATETIME,DELIVERY_DATETIME,SENDER_ID,B_NUMBER,MESSAGE_ID,TRANSACTION_ID,CHILD_USER_NAME,SMSC_ID,ERROR_CODE_ID,SMSC_ERROR_CODE,
ERROR_CODE_DESCRIPTION,ENCODING,DLT_PE_ID,DLT_CT_ID,PARTS,OPERATOR,CIRCLE_NAME,PLATFORM,SMS_CONTENT,FILE_NAME);" 2>/dev/null
rm -f $i
done

rm -fv ${path1}/lock.log

