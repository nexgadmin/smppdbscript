#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/file_cute
export path2=/data01/shfiles/file_cute/cdrs
export path3=/data01/shfiles/file_cute/seqn
export path4=/data01/shfiles/file_cute/load
export path5=/backup/nexg
export path6=/data01/shfiles/materialized_view/cdrs
export path7=/data01/shfiles/file_cute/bad

kill1=$(ps -eo comm,pid,etimes,cmd | grep file_cute.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

if [ -f ${path1}/lock.log ]
then
find ${path1}/lock.log -type f -mmin +35 -exec rm -rvf {} \;
echo "SCRIPT ALREADY RUNNING PROCESS EXIT FROM HERE"
exit
fi

day0=`date --date="-0 days" +%Y_%m_%d`

if [ -d ${path5}/$day0 ]
then
echo "Dir Exist"
echo ""
else
mkdir -p ${path5}/$day0
fi

cd ${path1}

echo "1" >${path1}/lock.log

rm -f ${path2}/*.csv
rm -f ${path4}/*.txt

seq1=$(cat ${path3}/file_seq.txt | head -1)

if [[ 10#$seq1 -eq 10000 ]]
then
seq1=0001
fi

min1=$(cat ${path3}/cdrs_seq.txt | head -1)

cnt1=$(mysql -sN -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel -e "SELECT MAX(sql_id),COUNT(1) FROM sent_sms WHERE momt='MT' AND sql_id >=$min1;" 2>/dev/null | sed 's/\t/|/g;')
cnt2=$(echo $cnt1 | awk -F"|" '{print $2}')
max1=$(echo $cnt1 | awk -F"|" '{if($1=="NULL") print 0} {if($1!="NULL") print $1}')

echo min_seq = $min1 :: max_seq = $max1 :: cdrs_count = $cnt2

if [[ $cnt2 -gt 0 ]]
then

date1=$(date +\%Y\%m\%d_\%H\%M\%S_)
echo $date1$seq1 "CDRs DownLoad"

mysql -sN -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel -e "
SELECT '"$date1$seq1"' AS file_name,DATE(FROM_UNIXTIME(TIME)) AS event_date,FROM_UNIXTIME(TIME) AS submit_time,UPPER(sender) AS sender,
REPLACE(receiver,'+','') AS receiver,REPLACE(dlr_url,'|','-') AS transaction_id,smsc_id,UPPER(service) AS service,coding,IFNULL(validity,0) AS validity,
CASE WHEN POSITION('peid=' IN meta_data)=0 THEN '0' ELSE SUBSTRING_INDEX(SUBSTRING_INDEX(meta_data,'peid=',-1),'&',1) END AS entity_id,
CASE WHEN POSITION('contentid=' IN meta_data)=0 THEN '0' ELSE SUBSTRING_INDEX(SUBSTRING_INDEX(meta_data,'contentid=',-1),'&',1) END AS content_id,
UPPER(SUBSTRING_INDEX(smsc_id,'_',1)) AS smsc,msgdata
FROM sent_sms WHERE momt='MT' AND sql_id BETWEEN $min1 AND $max1;" 2>/dev/null | sed 's/\t/|/g;' >${path2}/MT.csv

mysql -sN -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel -e "
SELECT '"$date1$seq1"' AS file_name,FROM_UNIXTIME(TIME) AS dlr_time,REPLACE(IFNULL(foreign_id,0),'|','-') AS message_id,
IFNULL(account,0) AS account,REPLACE(dlr_url,'|','-') AS transaction_id,msgdata
FROM sent_sms WHERE momt='DLR' AND sql_id BETWEEN $min1 AND $max1;" 2>/dev/null | sed 's/\t/|/g;' >${path2}/DLR.csv

seq2=$((10#$seq1+1))
echo $seq2 | awk '{print sprintf("%0'4'd", $0)}' >${path3}/file_seq.txt
echo $max1 | awk '{print $0+1}' >${path3}/cdrs_seq.txt

cd ${path2}

python3 ${path1}/mt.py MT.csv | tr -d '\r' | awk -F"|" '{print $0}' | tr -d '"' >${path4}/MT.txt
python3 ${path1}/dlr.py DLR.csv | tr -d '\r' | awk -F"|" '{print $2"|"$3"|"$7"|"$6"|"$4"|"$5}' >${path4}/DLR.txt

find ${path4}/ -type f -size 0 -exec rm -rvf {} \;

cd ${path4}

if [ -f MT.txt ]
then
mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching --local-infile -vvv -w -e "LOAD DATA local INFILE 'MT.txt' INTO TABLE NEXG_CDRS_STITCHING
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
(FILE_NAME,EVENT_DATE,SUBMIT_DATETIME,SENDER_ID,B_NUMBER,MESSAGE_LENGTH,TRANSACTION_ID,SMSC_NAME,CHILD_USER_NAME,ENCODING,VALIDITY,DLT_PE_ID,DLT_CT_ID,SMSC,PARTS,SMS_CONTENT);" 2>/dev/null
fi

if [ -f DLR.txt ]
then
mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching --local-infile -vvv -w -e "LOAD DATA local INFILE 'DLR.txt' INTO TABLE NEXG_DLR
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
(DELIVERY_DATETIME,MESSAGE_ID,DESCRIPTION,SMSC_ERROR_CODE,SMSC_USER_NAME,TRANSACTION_ID);" 2>/dev/null
fi

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching -vvv -e "
UPDATE stitching.NEXG_CDRS_STITCHING A, stitching.NEXG_DLR B SET A.DELIVERY_DATETIME=B.DELIVERY_DATETIME,A.MESSAGE_ID=B.MESSAGE_ID,A.DESCRIPTION=B.DESCRIPTION,A.SMSC_ERROR_CODE=B.SMSC_ERROR_CODE,A.SMSC_USER_NAME=B.SMSC_USER_NAME WHERE A.TRANSACTION_ID=B.TRANSACTION_ID;
UPDATE stitching.NEXG_CDRS_STITCHING A INNER JOIN sms_cdrs.LEVEL B ON MID(A.B_NUMBER,3,5)=B.LEVEL SET A.LRN=B.LRN,A.OPERATOR=B.OPERATOR,A.CIRCLE_NAME=B.CIRCLE_NAME WHERE A.LRN='0';
UPDATE stitching.NEXG_CDRS_STITCHING A INNER JOIN sms_cdrs.LEVEL B ON MID(A.B_NUMBER,3,4)=B.LEVEL SET A.LRN=B.LRN,A.OPERATOR=B.OPERATOR,A.CIRCLE_NAME=B.CIRCLE_NAME WHERE A.LRN='0';
UPDATE stitching.NEXG_CDRS_STITCHING SET OPERATOR='UNKNOWN' WHERE OPERATOR IS NULL;
UPDATE stitching.NEXG_CDRS_STITCHING A, stitching.SMPP_DLR_ERR_MAPPING B SET A.ERROR_CODE_DESCRIPTION=B.ERROR_CODE_DESCRIPTION WHERE A.SMSC_ERROR_CODE=B.ACTUAL_DLR_ERR AND A.SMSC=B.SMSC;
UPDATE stitching.NEXG_CDRS_STITCHING A, stitching.SMSC_ID_MST_TEMP B SET A.SMSC_USER_NAME=B.USER_NAME,A.DELIVERY_DATETIME=NOW(),A.DESCRIPTION='EXPIRED',A.ERROR_CODE_DESCRIPTION='Dlr Not Recieved On Time',A.SMSC_ERROR_CODE='9999' WHERE A.SMSC_NAME=B.SMSC_ID AND A.DESCRIPTION='PENDING' AND TIMESTAMPDIFF(SECOND,A.SUBMIT_DATETIME,NOW())>43200;
UPDATE stitching.NEXG_CDRS_STITCHING SET DESCRIPTION='REJECTD' WHERE DESCRIPTION NOT IN ('REJECTD','DELIVRD','UNDELIV','EXPIRED','PENDING');
UPDATE stitching.NEXG_CDRS_STITCHING SET SMSC_ERROR_CODE='000' WHERE DESCRIPTION='DELIVRD';

INSERT INTO sms_cdrs.NEXG_CDRS (EVENT_DATE,SUBMIT_DATETIME,DELIVERY_DATETIME,SENDER_ID,B_NUMBER,MESSAGE_LENGTH,MESSAGE_ID,TRANSACTION_ID,SMSC,SMSC_NAME,CHILD_USER_NAME,SMSC_USER_NAME,SMSC_ERROR_CODE,DESCRIPTION,ERROR_CODE_DESCRIPTION,ENCODING,VALIDITY,DLT_PE_ID,DLT_CT_ID,PARTS,LRN,OPERATOR,CIRCLE_NAME,FILE_NAME)
SELECT EVENT_DATE,SUBMIT_DATETIME,DELIVERY_DATETIME,SENDER_ID,B_NUMBER,MESSAGE_LENGTH,MESSAGE_ID,TRANSACTION_ID,SMSC,SMSC_NAME,CHILD_USER_NAME,SMSC_USER_NAME,SMSC_ERROR_CODE,DESCRIPTION,ERROR_CODE_DESCRIPTION,ENCODING,VALIDITY,DLT_PE_ID,DLT_CT_ID,PARTS,LRN,OPERATOR,CIRCLE_NAME,'"$date1$seq1"' AS FILE_NAME
FROM NEXG_CDRS_STITCHING WHERE DESCRIPTION!='PENDING';
" 2>/dev/null

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching -e "SELECT EVENT_DATE,SUBMIT_DATETIME,DELIVERY_DATETIME,SENDER_ID,
B_NUMBER,MESSAGE_LENGTH,MESSAGE_ID,TRANSACTION_ID,SMSC,SMSC_NAME,CHILD_USER_NAME,SMSC_USER_NAME,SMSC_ERROR_CODE,DESCRIPTION,ERROR_CODE_DESCRIPTION,
ENCODING,VALIDITY,DLT_PE_ID,DLT_CT_ID,PARTS,LRN,OPERATOR,CIRCLE_NAME,'"$date1$seq1"' AS FILE_NAME,SMS_CONTENT 
FROM NEXG_CDRS_STITCHING WHERE DESCRIPTION!='PENDING';" 2>/dev/null | sed 's/\t/|/g;' >${path5}/$day0/$date1$seq1.txt

mysql -h172.19.8.163 -uroot -p'Vp$9~ioKy6t^8&e' nexgmis --local-infile -vvv -w -e "LOAD DATA local INFILE '${path5}/$day0/$date1$seq1.txt'
INTO TABLE NEXG_CDRS FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' IGNORE 1 LINES
(EVENT_DATE,SUBMIT_DATETIME,DELIVERY_DATETIME,SENDER_ID,B_NUMBER,MESSAGE_LENGTH,MESSAGE_ID,TRANSACTION_ID,SMSC,SMSC_NAME,CHILD_USER_NAME,
SMSC_USER_NAME,SMSC_ERROR_CODE,DESCRIPTION,ERROR_CODE_DESCRIPTION,ENCODING,VALIDITY,DLT_PE_ID,DLT_CT_ID,PARTS,LRN,OPERATOR,
CIRCLE_NAME,FILE_NAME,SMS_CONTENT);" 2>/dev/null

cat ${path5}/$day0/$date1$seq1.txt | sed 's/,/;/g' | sed 's/|/,/g' | mongoimport --host=172.19.8.62:27017 -u root -p 'p*E9@r#Xnh' --authenticationDatabase=admin --db=admin --collection=NEXG_CDRS --type=csv --headerline

gzip ${path5}/$day0/$date1$seq1.txt

mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching -e "
SELECT EVENT_DATE,SUBMIT_DATETIME,DELIVERY_DATETIME,SENDER_ID,B_NUMBER,MESSAGE_ID,TRANSACTION_ID,CHILD_USER_NAME,SMSC_NAME,
SMSC_ERROR_CODE,SMSC_ERROR_CODE,ERROR_CODE_DESCRIPTION,ENCODING,DLT_PE_ID,DLT_CT_ID,PARTS,OPERATOR,CIRCLE_NAME,'NEXG' AS PLATFORM,
SMS_CONTENT,'"$date1$seq1"' AS FILE_NAME
FROM NEXG_CDRS_STITCHING WHERE DESCRIPTION!='PENDING';" 2>/dev/null | sed 's/\t/|/g;' >${path6}/$date1$seq1.tmp

mv ${path6}/$date1$seq1.tmp ${path6}/$date1$seq1.csv

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching -vvv -e "DELETE FROM NEXG_CDRS_STITCHING WHERE DESCRIPTION!='PENDING';" 2>/dev/null
mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching -vvv -e "DELETE FROM NEXG_DLR WHERE CDR_LOAD_DATE < (NOW() - INTERVAL 1 HOUR);" 2>/dev/null

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching -e "SELECT * FROM stitching.NEXG_CDRS_STITCHING;" 2>/dev/null | sed 's/\t/|/g;' >${path4}/PENDING_CDRS.txt

mysql -h172.19.8.163 -uroot -p'Vp$9~ioKy6t^8&e' nexgmis -e "TRUNCATE TABLE NEXG_CDRS_PENDING;" 2>/dev/null

mysql -h172.19.8.163 -uroot -p'Vp$9~ioKy6t^8&e' nexgmis --local-infile -vvv -w -e "LOAD DATA local INFILE '${path4}/PENDING_CDRS.txt'
INTO TABLE NEXG_CDRS_PENDING FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' IGNORE 1 LINES
(EVENT_DATE,SUBMIT_DATETIME,DELIVERY_DATETIME,SENDER_ID,B_NUMBER,MESSAGE_LENGTH,MESSAGE_ID,TRANSACTION_ID,SMSC_NAME,CHILD_USER_NAME,
SMSC_USER_NAME,SMSC_ERROR_CODE,DESCRIPTION,ERROR_CODE_DESCRIPTION,ENCODING,VALIDITY,DLT_PE_ID,DLT_CT_ID,PARTS,LRN,OPERATOR,
CIRCLE_NAME,SMSC,SMS_CONTENT,FILE_NAME,CDR_LOAD_DATE);" 2>/dev/null

echo ""
echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec NexG CDRs
echo ""
echo ""

cd ${path4}

datepart0=$(date --date="-0 days" +%Y%m%d | awk '{print "DATE_"int($0)}')

mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT EVENT_DATE,CHILD_USER_NAME,SMSC_NAME,COUNT(1) AS STATUS_SUBMITION,COUNT(1) AS STATUS_DLR,
SUM(CASE WHEN DESCRIPTION='DELIVRD' THEN PARTS ELSE 0 END) AS STATUS_DELIVERED,
SUM(CASE WHEN SMSC_ERROR_CODE!='9998' AND DESCRIPTION!='DELIVRD' THEN PARTS ELSE 0 END) AS STATUS_FAILED,
SUM(CASE WHEN DESCRIPTION='PENDING' THEN PARTS ELSE 0 END) AS STATUS_PENDING,
SUM(CASE WHEN SMSC_ERROR_CODE='9998' THEN PARTS ELSE 0 END) AS STATUS_NACK
FROM (SELECT EVENT_DATE,SENDER_ID,CHILD_USER_NAME,SMSC_ERROR_CODE,DESCRIPTION,PARTS,SMSC_USER_NAME,SMSC_NAME FROM sms_cdrs.NEXG_CDRS PARTITION("$datepart0")
UNION ALL
SELECT EVENT_DATE,SENDER_ID,CHILD_USER_NAME,SMSC_ERROR_CODE,DESCRIPTION,PARTS,SMSC_USER_NAME,SMSC_NAME FROM stitching.NEXG_CDRS_STITCHING WHERE EVENT_DATE>=SUBDATE(CURRENT_DATE(),0)) A
GROUP BY 1,2,3" 2>/dev/null | sed 's/\t/|/g;' >sms_summary.txt

mysql -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel -vvv -w -e"DELETE FROM sms_summary WHERE submit_date = SUBDATE(CURRENT_DATE(),0);" 2>/dev/null

mysql -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel --local-infile -vvv -w -e "LOAD DATA local INFILE 'sms_summary.txt'
INTO TABLE sms_summary FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' 
(submit_date,account,smsc,submit_count,dlr_count,status_delivered,status_failed,status_pending,status_nack);" 2>/dev/null

datepart1=$(date --date="-0 days" +%Y%m%d | awk '{print "DATE_"int($0)}')
mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -vvv -e "
ALTER TABLE ERROR_CODE_SUMMARY TRUNCATE PARTITION $datepart1;

INSERT INTO ERROR_CODE_SUMMARY (EVENT_DATE,USER_NAME,SMSC_ID,ERROR_CODE_ID,SMSC_STATUS_CODE,ERROR_DESCRIPTION,PLATFORM,CNT)
SELECT cdt.EVENT_DATE,UPPER(cdt.CHILD_USER_NAME) AS USER_NAME,CASE WHEN cdt.SMSC_ID='' THEN '0' ELSE cdt.SMSC_ID END AS SMSC_ID,cdt.ERROR_CODE_ID, cdt.SMSC_ERROR_CODE,(CASE WHEN sce.errorcode_short_desc IS NOT NULL THEN sce.errorcode_short_desc ELSE cdt.ERROR_CODE_DESCRIPTION END) AS ERROR_DESCRIPTION,cdt.PLATFORM,SUM(cdt.PARTS) AS CNT FROM CDRS_TEXT PARTITION("$datepart1") cdt LEFT JOIN sms_error_code sce ON cdt.ERROR_CODE_ID = sce.error_code WHERE cdt.ERROR_CODE_ID NOT IN ('0','000') AND cdt.CHILD_USER_NAME != '' GROUP BY 1,2,3,4,5,6;

UPDATE ERROR_CODE_SUMMARY PARTITION("$datepart1") A, SMSC_ID_MST B SET A.SMSC_NAME=B.SMSC_NAME
WHERE A.SMSC_ID=B.SMSC_ID AND A.PLATFORM=B.PLATFORM AND A.SMSC_NAME IS NULL;

ALTER TABLE HEADER_REPORT TRUNCATE PARTITION $datepart1; 

INSERT INTO HEADER_REPORT (EVENT_DATE,SENDER_ID,USER_NAME,DLT_PE_ID,DLT_CT_ID,SUBMITION,DELIVERED,PLATFORM)
SELECT EVENT_DATE,UPPER(SENDER_ID) AS SENDER_ID,UPPER(IFNULL(NULLIF(CHILD_USER_NAME,''),'NEXGCP1')) AS USER_NAME,DLT_PE_ID,DLT_CT_ID,
SUM(PARTS) AS SUBMITION,SUM(CASE WHEN ERROR_CODE_ID IN('0','000') THEN PARTS ELSE 0 END) AS DELIVERED,PLATFORM
FROM CDRS_TEXT PARTITION("$datepart1") GROUP BY 1,2,3,4,5;
" 2>/dev/null

else
echo "No Recode Found Process Exist from Here"
fi

rm -fv ${path1}/lock.log

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo min_seq = $min1 :: max_seq = $max1 :: cdrs_count = $cnt2
echo "Pending "$(mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching -e "SELECT COUNT(1) AS CNT FROM NEXG_CDRS_STITCHING;" 2>/dev/null)
echo ""
