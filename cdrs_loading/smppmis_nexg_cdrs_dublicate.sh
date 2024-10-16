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
        
        echo ":::::::::::::::::::INSERT INTO NEXG_CDRS_copy SELECT * FROM nexgmis.NEXG_CDRS
         partition("$datepart1") WHERE CHILD_USER_NAME ='$customer_name' group by TRANSACTION_ID::::::::::::::::::"
        
        mysql -h172.19.8.163 -uroot -p'Vp$9~ioKy6t^8&e' nexgmis -e "
        INSERT INTO NEXG_CDRS_copy SELECT * FROM nexgmis.NEXG_CDRS partition("$datepart1")
        WHERE CHILD_USER_NAME ='$customer_name' group by TRANSACTION_ID; " 2>/dev/null

        echo ":::::::::::::::::::END INSERT NEXG_CDRS_copy TABLE::::::::::::::::::"

        mysql -h172.19.8.163 -uroot -p'Vp$9~ioKy6t^8&e' nexgmis -e "
        DELETE FROM nexgmis.NEXG_CDRS PARTITION("$datepart1") WHERE CHILD_USER_NAME = '$customer_name'; " 2>/dev/null

        echo ":::::::::::::::::::END DELETE NEXG_CDRS By childUser TABLE::::::::::::::::::"

        mysql -h172.19.8.163 -uroot -p'Vp$9~ioKy6t^8&e' nexgmis -e "
        INSERT INTO NEXG_CDRS SELECT * FROM nexgmis.NEXG_CDRS_copy partition("$datepart1") Where CHILD_USER_NAME ='$customer_name' group by TRANSACTION_ID;
        
        UPDATE NEXG_CDRS partition("$datepart1") set DESCRIPTION = 'Delivered',ERROR_CODE_DESCRIPTION='Delivered' where CHILD_USER_NAME ="$customer_name" AND SMSC_ERROR_CODE="000";
        
        ALTER TABLE NEXG_CDRS_copy TRUNCATE PARTITION $datepart1;" 2>/dev/null;

        echo ":::::::::::::::::::END Inseert NEXG_CDRS By trancate copy table ::::::::::::::::::"

        echo ":::::::::::::::::::END SMPP dublicate data::::::::::::::::::"
    
    
    done < "${path2}/nexg_customer.txt"
else
    echo "File does not exist."
fi
