#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

date1=$(date +\%Y\%m\%d_\%H\%M\%S)

mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh stitching 2>/dev/null >/backup/database/stitching/$date1"_stitching.sql"
mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh --no-data sms_cdrs 2>/dev/null >/backup/database/sms_cdrs/$date1"_sms_cdrs.sql"
mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh nexg_hunt 2>/dev/null > /backup/database/nexg_hunt/$date1"_nexg_hunt.sql"

mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs BILLING_SUMMARY 2>/dev/null > /backup/database/sms_cdrs/$date1"_BILLING_SUMMARY.sql"
mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs CRM_RATE_MST_copy 2>/dev/null > /backup/database/sms_cdrs/$date1"_CRM_RATE_MST_copy.sql"
mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs DAILY_SUMMARY 2>/dev/null > /backup/database/sms_cdrs/$date1"_DAILY_SUMMARY.sql"
mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs ERROR_CODE_SUMMARY 2>/dev/null > /backup/database/sms_cdrs/$date1"_ERROR_CODE_SUMMARY.sql"
mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs GOVT_CLI 2>/dev/null > /backup/database/sms_cdrs/$date1"_GOVT_CLI.sql"
mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs HEADER_REPORT 2>/dev/null > /backup/database/sms_cdrs/$date1"_HEADER_REPORT.sql"
mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs LEVEL 2>/dev/null > /backup/database/sms_cdrs/$date1"_LEVEL.sql"
mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs PAYOUT_MST 2>/dev/null > /backup/database/sms_cdrs/$date1"_PAYOUT_MST.sql"
mysqldump -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs SMSC_ID_MST 2>/dev/null > /backup/database/sms_cdrs/$date1"_SMSC_ID_MST.sql"

find /backup/database/nexg_hunt/*.sql -type f -daystart -mtime +7 -exec rm -rvf {} \;
find /backup/database/sms_cdrs/*.sql -type f -daystart -mtime +7 -exec rm -rvf {} \;
find /backup/database/stitching/*.sql -type f -daystart -mtime +7 -exec rm -rvf {} \;
find /data01/mysql_files/*.csv -type f -daystart -mtime +7 -exec rm -rvf {} \;

mysql -h172.19.4.103 -uroot -pp*E9@r#Xnh nexg_hunt -vvv -e "DELETE FROM REPORTS_DASHBOARD WHERE EVENT_DATE < SUBDATE(CURRENT_DATE(),7);" 2>/dev/null

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
