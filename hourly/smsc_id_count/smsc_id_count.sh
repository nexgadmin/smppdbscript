#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/hourly/smsc_id_count

kill1=$(ps -eo comm,pid,etimes,cmd | grep smsc_id_count.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

##########################################smpp_smsc#######################################
cd ${path1}
rm -f ${path1}/*.txt

mysql -sN -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel -e "SELECT * FROM smpp_smsc;" 2>/dev/null | sed 's/\t/|/g;' >${path1}/smpp.txt

cat smpp.txt | awk -F"|" '{split($2,a,"_"); print $2"|"$7"|"toupper(a[1])"|NEXG|system"}' >${path1}/smpp1.txt

find ${path1}/ -type f -size 0 -exec rm -rvf {} \;

if [ -f ${path1}/smpp1.txt ]
then
mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching --local-infile -vvv -w -e "LOAD DATA local INFILE 'smpp1.txt' REPLACE INTO TABLE SMSC_ID_MST_TEMP
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
(SMSC_ID,USER_NAME,SMSC_NAME,PLATFORM,CREATED_BY);" 2>/dev/null
fi

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching -vvv -e "
UPDATE stitching.SMSC_ID_MST_TEMP SET SMSC_NAME='JIO CLOUD' WHERE SMSC_NAME='JIO' AND SMSC_ID IN('Jio_Cloud_TRN','Jio_Cloud_PRO','Jio_Cloud_OTP','Jio_DC_TRN','Jio_DC_PRO','Jio_DC_OTP','Jio_MC3_TRN','Jio_MC2_OTP','9395','9394','9389','9396');
UPDATE stitching.SMSC_ID_MST_TEMP SET SMSC_NAME='RELIANCE JIO' WHERE SMSC_NAME='JIO';
INSERT IGNORE INTO sms_cdrs.SMSC_ID_MST SELECT * FROM stitching.SMSC_ID_MST_TEMP;
" 2>/dev/null

##########################################smsc_id_count#######################################

cd ${path1}

datepart1=$(date --date="-300 minutes" +%Y%m%d | awk '{print "DATE_"int($0)}')

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
SELECT A.EVENT_DATE,A.PLATFORM,A.SMSC_ID,B.SMSC_NAME,SUBMITION,DELIVERED,(SUBMITION - DELIVERED) AS FAILED FROM
(SELECT LEFT(SUBMIT_DATETIME,13) AS EVENT_DATE,PLATFORM,SMSC_ID,SUM(PARTS) AS SUBMITION,
SUM(CASE WHEN ERROR_CODE_ID IN('0','000') THEN PARTS ELSE 0 END) AS DELIVERED
FROM CDRS_TEXT PARTITION("$datepart1") GROUP BY 1,2,3) A LEFT JOIN SMSC_ID_MST B ON B.SMSC_ID=A.SMSC_ID AND B.PLATFORM=A.PLATFORM
WHERE A.SMSC_ID NOT IN ('','0','NULL');" 2>/dev/null | sed 's/\t/|/g;' >${path1}/mail.txt

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
