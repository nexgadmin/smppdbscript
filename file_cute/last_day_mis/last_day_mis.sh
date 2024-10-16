#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/file_cute/last_day_mis

kill1=$(ps -eo comm,pid,etimes,cmd | grep last_day_mis.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

cd ${path1}

for i in `seq 5 -1 1`
do
datepart0=$(date --date="-$i days" +%Y%m%d | awk '{print "DATE_"int($0)}')
echo $datepart0
rm -f *.txt

mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE AS submit_hour,CHILD_USER_NAME AS account,SMSC_ERROR_CODE AS reason,SUM(PARTS) AS COUNT
FROM sms_cdrs.NEXG_CDRS PARTITION("$datepart0") WHERE DESCRIPTION!='DELIVRD'
GROUP BY 1,2,3;" 2>/dev/null | sed 's/\t/|/g;' >sms_summary_error.txt

mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE,CHILD_USER_NAME,SMSC_NAME,SUM(PARTS) AS STATUS_SUBMITION,SUM(PARTS) AS STATUS_DLR,
SUM(CASE WHEN DESCRIPTION='DELIVRD' THEN PARTS ELSE 0 END) AS STATUS_DELIVERED,
SUM(CASE WHEN SMSC_ERROR_CODE!='9998' AND DESCRIPTION!='DELIVRD' THEN PARTS ELSE 0 END) AS STATUS_FAILED,
SUM(CASE WHEN DESCRIPTION='PENDING' THEN PARTS ELSE 0 END) AS STATUS_PENDING,
SUM(CASE WHEN SMSC_ERROR_CODE='9998' THEN PARTS ELSE 0 END) AS STATUS_NACK
FROM sms_cdrs.NEXG_CDRS PARTITION("$datepart0")
GROUP BY 1,2,3" 2>/dev/null | sed 's/\t/|/g;' >sms_summary_smsc.txt

mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE,CHILD_USER_NAME,SENDER_ID,SUM(PARTS) AS STATUS_SUBMITION,SUM(PARTS) AS STATUS_DLR,
SUM(CASE WHEN DESCRIPTION='DELIVRD' THEN PARTS ELSE 0 END) AS STATUS_DELIVERED,
SUM(CASE WHEN SMSC_ERROR_CODE!='9998' AND DESCRIPTION!='DELIVRD' THEN PARTS ELSE 0 END) AS STATUS_FAILED,
SUM(CASE WHEN DESCRIPTION='PENDING' THEN PARTS ELSE 0 END) AS STATUS_PENDING,
SUM(CASE WHEN SMSC_ERROR_CODE='9998' THEN PARTS ELSE 0 END) AS STATUS_NACK
FROM sms_cdrs.NEXG_CDRS PARTITION("$datepart0")
GROUP BY 1,2,3" 2>/dev/null | sed 's/\t/|/g;' >sms_summary_header.txt

mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE,CHILD_USER_NAME,SMSC_NAME,SUM(PARTS) AS STATUS_SUBMITION,SUM(PARTS) AS STATUS_DLR,
SUM(CASE WHEN DESCRIPTION='DELIVRD' THEN PARTS ELSE 0 END) AS STATUS_DELIVERED,
SUM(CASE WHEN SMSC_ERROR_CODE!='9998' AND DESCRIPTION!='DELIVRD' THEN PARTS ELSE 0 END) AS STATUS_FAILED,
SUM(CASE WHEN DESCRIPTION='PENDING' THEN PARTS ELSE 0 END) AS STATUS_PENDING,
SUM(CASE WHEN SMSC_ERROR_CODE='9998' THEN PARTS ELSE 0 END) AS STATUS_NACK
FROM sms_cdrs.NEXG_CDRS PARTITION("$datepart0")
GROUP BY 1,2,3" 2>/dev/null | sed 's/\t/|/g;' >sms_summary.txt

mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE,CHILD_USER_NAME,CAMPAIGN,SUM(PARTS) AS STATUS_SUBMITION,SUM(PARTS) AS STATUS_DLR,
SUM(CASE WHEN DESCRIPTION='DELIVRD' THEN PARTS ELSE 0 END) AS STATUS_DELIVERED,
SUM(CASE WHEN SMSC_ERROR_CODE!='9998' AND DESCRIPTION!='DELIVRD' THEN PARTS ELSE 0 END) AS STATUS_FAILED,
SUM(CASE WHEN DESCRIPTION='PENDING' THEN PARTS ELSE 0 END) AS STATUS_PENDING,
SUM(CASE WHEN SMSC_ERROR_CODE='9998' THEN PARTS ELSE 0 END) AS STATUS_NACK
FROM sms_cdrs.NEXG_CDRS PARTITION("$datepart0")
GROUP BY 1,2,3" 2>/dev/null | sed 's/\t/|/g;' >sms_summary_campaign.txt

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh smpp -vvv -w -e"
DELETE FROM sms_summary WHERE submit_date = SUBDATE(CURRENT_DATE(),"$i");
DELETE FROM sms_summary_campaign WHERE submit_hour = SUBDATE(CURRENT_DATE(),"$i");
DELETE FROM sms_summary_error WHERE submit_hour = SUBDATE(CURRENT_DATE(),"$i");
DELETE FROM sms_summary_header WHERE submit_hour = SUBDATE(CURRENT_DATE(),"$i");
DELETE FROM sms_summary_smsc WHERE submit_hour = SUBDATE(CURRENT_DATE(),"$i");
" 2>/dev/null

mysql -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel --local-infile -vvv -w -e "LOAD DATA local INFILE 'sms_summary_error.txt' INTO TABLE sms_summary_error FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (submit_hour,account,error,count);"
mysql -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel --local-infile -vvv -w -e "LOAD DATA local INFILE 'sms_summary_smsc.txt' INTO TABLE sms_summary_smsc FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (submit_hour,account,smsc,submit_count,dlr_count,status_delivered,status_failed,status_pending,status_nack);"
mysql -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel --local-infile -vvv -w -e "LOAD DATA local INFILE 'sms_summary_header.txt' INTO TABLE sms_summary_header FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (submit_hour,account,header,submit_count,dlr_count,status_delivered,status_failed,status_pending,status_nack);"
mysql -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel --local-infile -vvv -w -e "LOAD DATA local INFILE 'sms_summary.txt' INTO TABLE sms_summary FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (submit_date,account,smsc,submit_count,dlr_count,status_delivered,status_failed,status_pending,status_nack);"
mysql -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel --local-infile -vvv -w -e "LOAD DATA local INFILE 'sms_summary_campaign.txt' INTO TABLE sms_summary_campaign FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (submit_hour,account,campaign,submit_count,dlr_count,status_delivered,status_failed,status_pending,status_nack);"

done

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
