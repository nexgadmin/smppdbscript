#!/bin/bash

start1=$(date)
echo "$start1 StartDateTime"
echo ""

# Oracle DB Variables
sqllogin="TELELINKPRD/TELE#235#LINK#56#PRD@172.19.4.101:1521/nexggold"

export path1=/data01/shfiles/ra_reports/waba_report

cd "${path1}"
rm -f "${path1}"/*.txt

# Run SQLPlus script to extract data from Oracle
sqlplus -s "${sqllogin}" << EOF > "${path1}/sqlplus.log" 2>&1
SET ECHO OFF;
SET HEADING OFF;
SET PAGESIZE 0;
SET TERMOUT OFF;
SET VERIFY OFF;
SET LINESIZE 2500;
SET FEEDBACK OFF;
SET TRIMS ON;
SET NULL 0;

SPOOL ${path1}/waba1.txt;

SELECT DISTINCT CUSTOMER_CODE || '|' || CUSTOMER_NAME
FROM customer_mst
WHERE service_type = 'S011';

SPOOL OFF;
exit;
EOF

# Clean up empty files
find "${path1}"/*.txt -type f -size 0 -exec rm -rvf {} \;

# Load data into MySQL table
if [ -f "${path1}/waba1.txt" ]; then
    mysql -h 172.19.8.164 -uroot -p'Vp$9~ioKy6t^8&e' nexgreach --local-infile -vvv -w -e \
    "LOAD DATA LOCAL INFILE '${path1}/waba1.txt' INTO TABLE crm_accounts FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (crm_id, customer_name);" 2>/dev/null

    # Execute MySQL query to get message counts and store in mail.txt
    mysql -h172.19.8.164 -uroot -p'Vp$9~ioKy6t^8&e' nexgreach -e "
    SELECT crm_accounts.customer_name, COUNT(*) AS message_count 
    FROM crm_accounts 
    INNER JOIN whatsappdelivery ON crm_accounts.user_uid = whatsappdelivery.user_id 
    WHERE whatsappdelivery.submittime BETWEEN (CURRENT_DATE() - INTERVAL 1 MONTH) AND CURRENT_DATE() 
    GROUP BY crm_accounts.customer_name;" 2>/dev/null > sed 's/\t/|/g' > "${path1}/mail.txt"

    # Clean up empty mail.txt file
    find "${path1}/mail.txt" -type f -size 0 -exec rm -rvf {} \;

    # If mail.txt exists, send email using Java application
    #if [ -f "${path1}/mail.txt" ]; then
       # java -jar SendMail.jar 2
    #fi
fi

echo ""
end1=$(date)
echo "$end1 EndDateTime"
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$start1" "+%s") )))
echo "$timediff1 Sec"
echo ""
