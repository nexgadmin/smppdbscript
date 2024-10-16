#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/infobip_loading
export path2=/data01/shfiles/infobip_loading/cdrs
export path3=/var/livecdrs/ftp_data/infobip
export path4=/backup/infobip
export path5=/data01/shfiles/materialized_view/cdrs/infobip
export path6=/data01/shfiles/infobip_loading/reco

echo "1" >${path1}/lock.log

day0=`date --date="-0 days" +%Y_%m_%d`

if [ -d ${path4}/$day0 ]
then
echo "Dir Exist"
echo ""
else
mkdir -p ${path4}/$day0
fi

cd ${path3}
files=$(ls -1 | grep ".*\.zip$" | wc -l)

if [[ $files -gt 0 ]]
then
for i in `ls -1 | grep ".*\.zip$"`
do
file=$(echo $i | awk '{gsub(/.zip/,"",$0); print $0}')
gunzip -c $i | tr -d '\r' | awk -v RS='"' 'NR % 2 == 0 { gsub(/\n/, "") } { printf("%s%s", $0, RT) }' | awk -F"|" '{if(substr($2,1,2)=="20") print "'$file'""|"$0}' | sed 's/\\/#/g;s/|//29;' | sed 's/|//29;' | sed 's/|//29;' | sed 's/|//29;' | tr -d '"' >${path2}/$file.csv.tmp 
mv -f ${path2}/$file.csv.tmp ${path2}/$file.csv
mv -f $i ${path4}/$day0
done
fi

 
cd ${path2}
f1=$(ls -1 | grep ".*\.csv$" | wc -l)

if [[ $f1 -gt 0 ]]
then
for i in `ls -1 | grep ".*\.csv$"`
do
echo $i
mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs --local-infile -vvv -w -e "LOAD DATA local INFILE '$i' INTO TABLE INFOBIP_CDRS
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
(FILE_NAME,EVENT_DATE,TIMESTAMP,SENDER_ID,B_NUMBER,CAMPAIGN_ID,MESSAGE_ID,TRANSACTION_ID,PARENT_USER_NAME,CHILD_USER_NAME,SMSC_SUBMIT_DATE,
SMSC_SUBMIT_DATETIME,DELIVERY_DATETIME,ERROR_CODE_ID,SMSC_ERROR_CODE,ERROR_CODE_DESCRIPTION,DCS,GATEWAY_DATA,OPERATOR_ID,
INTERFACE_ID,OPERATOR,CIRCLE,DLT_PE_ID,DLT_CT_ID,RETRY_COUNT,MESSAGE_LENGTH,TOTAL_PART_OF_SMS,CRM_ID);" 2>/dev/null

cnt1=$(wc -l $i | awk '{print $1}')
echo $(date +\%Y\-%m\-%d\ %H\:%M\:%S)"|"$i"|"$cnt1 >>${path6}/CDRs_file_row_count_$day0.txt

mv -f $i ${path5}

done
fi

find ${path6}/*.txt -type f -daystart -mtime +7 -exec rm -rvf {} \;

rm -fv ${path1}/lock.log

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
