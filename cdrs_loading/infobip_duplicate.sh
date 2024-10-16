#!/bin/sh
path1=/data01/shfiles/cdrs_loading/view_log
echo "1" >${path1}/lock.log

export path2=/data01/shfiles/cdrs_loading/customers
datepart1="DATE_20240824"
echo "PartitionDate "$datepart1


find ${path2}/nexg_customer.txt -type f -size 0 -exec rm -rvf {} \;

if [ -f "${path2}/nexg_customer.txt" ]; then
    # Loop through each line in the file
    while IFS= read -r customer_name; do
        echo "Customer name: $customer_name"
        
        echo ":::::::::::::::::::INSERT INTO CDRS_TEXT_copy SELECT * FROM sms_cdrs.CDRS_TEXT
         partition("$datepart1") WHERE CHILD_USER_NAME ='$customer_name' group by TRANSACTION_ID::::::::::::::::::"
        
        mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
        INSERT INTO CDRS_TEXT_copy SELECT * FROM sms_cdrs.CDRS_TEXT partition("$datepart1")
        WHERE CHILD_USER_NAME ='$customer_name' group by TRANSACTION_ID; " 2>/dev/null

        echo ":::::::::::::::::::END INSERT CDRS_TEXT_copy TABLE::::::::::::::::::"

        mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
        DELETE FROM sms_cdrs.CDRS_TEXT PARTITION("$datepart1") WHERE CHILD_USER_NAME = '$customer_name'; " 2>/dev/null

        echo ":::::::::::::::::::END DELETE CDRS_TEXT By childUser TABLE::::::::::::::::::"

        mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
        INSERT INTO CDRS_TEXT SELECT * FROM sms_cdrs.CDRS_TEXT_copy partition("$datepart1") Where CHILD_USER_NAME ='$customer_name' group by TRANSACTION_ID;

        UPDATE sms_cdrs.CDRS_TEXT partition("$datepart1") set DESCRIPTION = 'DELIVRD', ERROR_CODE_DESCRIPTION='DELIVRD' 
        where CHILD_USER_NAME ='$customer_name' AND SMSC_ERROR_CODE='000';

        ALTER TABLE CDRS_TEXT_copy TRUNCATE PARTITION $datepart1;" 2>/dev/null;

        echo ":::::::::::::::::::END Inseert CDRS_TEXT By trancate copy table ::::::::::::::::::"

        echo ":::::::::::::::::::END SMPP dublicate data::::::::::::::::::"
    
    
    done < "${path2}/nexg_customer.txt"
else
    echo "File does not exist."
fi
