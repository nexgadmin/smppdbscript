#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo "" 

# Database credentials
DB_HOST="172.19.4.103"
DB_USER="root"
DB_PASS='p*E9@r#Xnh'
DB_NAME="sms_cdrs"
 

 
# Output file
export OUTPUT_FILE="/data01/shfiles/ra_reports/user_reco/output.txt"

# SQL query to find phone numbers
SQL_QUERY="SELECT EVENT_DATE, SUM(SUBMITION), SUM(DELIVERED), SMSC_NAME FROM sms_cdrs.BILLING_SUMMARY where EVENT_DATE between '2023-04-01' and '2024-03-31' group by 1,4;"

#SQL_QUERY="select SMSC_NAME from BILLING_SUMMARY group by 1,4;"

# Print the SQL query for debugging purposes
echo $SQL_QUERY


# Execute the SQL query and export the results to a pipe-separated text file
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" --batch  "$DB_NAME" -e "$SQL_QUERY" | sed 's/\t/|/g' > "$OUTPUT_FILE"
# Optionally, print the output file content for verification

cat "$OUTPUT_FILE"
 
# Add a header to the text file
sed -i '1iEVENT_DATE|SMSC_SUBMIT_DATETIME|DELIVERY_DATETIME|SENDER_ID|B_NUMBER|MESSAGE_ID|TRANSACTION_ID|SMSC_ERROR_CODE|ERROR_CODE_DESCRIPTION' "$OUTPUT_FILE

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
