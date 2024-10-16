#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

sqllogin="TELELINKPRD/TELE#235#LINK#56#PRD@172.19.4.101:1521/nexggold"

export path1=/data01/shfiles/crm_rate_master

kill1=$(ps -eo comm,pid,etimes,cmd | grep crm_rate.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

cd ${path1}
rm -f ${path1}/*.txt

sqlplus -s ${sqllogin} << EOF >/dev/null
SET ECHO OFF;
SET HEADING OFF;
SET PAGESIZE 0;
SET TERMOUT OFF;
SET VERIFY OFF;
SET LINESIZE 2500;
SET FEEDBACK OFF;
SET TRIMS ON;
SET NULL 0;

SPOOL ${path1}/customer_code.txt;
select customer_code, customer_name from customer_mst where service_type='S011' and status='Y' ;

SPOOL OFF;
exit;
EOF

