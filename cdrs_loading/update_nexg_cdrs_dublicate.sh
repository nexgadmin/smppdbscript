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
        
        echo ":::::::::::::::::::Started UPDATE NEXG_CDRS Dlr By childUser TABLE::::::::::::::::::"

        mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "
        UPDATE sms_cdrs.NEXG_CDRS partition("$datepart1") set DESCRIPTION = 'DELIVRD', ERROR_CODE_DESCRIPTION='DELIVRD' 
        where CHILD_USER_NAME ='$customer_name' AND SMSC_ERROR_CODE='000';" 2>/dev/null;

        echo "UPDATE sms_cdrs.NEXG_CDRS partition("$datepart1") set DESCRIPTION = 'DELIVRD', ERROR_CODE_DESCRIPTION='DELIVRD' 
        where CHILD_USER_NAME ='$customer_name' AND SMSC_ERROR_CODE='000';"
        echo " '''''''END UPDate nexg_cdrs table'''''''''''''"

        mysql -h172.19.8.163 -uroot -p'Vp$9~ioKy6t^8&e' nexgmis -e "
        UPDATE NEXG_CDRS partition("$datepart1") set DESCRIPTION = 'DELIVRD',ERROR_CODE_DESCRIPTION='Delivered'
         where CHILD_USER_NAME ='$customer_name' AND SMSC_ERROR_CODE='000'; " 2>/dev/null

        echo ":::::::::::::::::::END Update smppmis NEXG_CDRS By childUser TABLE::::::::::::::::::"

        echo ":::::::::::::::::::END SMPP dublicate data::::::::::::::::::"
    
    
    done < "${path2}/nexg_customer.txt"
else
    echo "File does not exist."
fi
