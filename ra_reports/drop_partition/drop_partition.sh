#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

export path1=/data01/shfiles/ra_reports/drop_partition

cd ${path1}

rm -f mail.txt

datepart1=$(date --date="-2 days" +%Y%m%d | awk '{print "DATE_"int($0)}')

S103=$(mysql -sN -h172.19.4.103 -uroot -pp*E9@r#Xnh sms_cdrs -e "SELECT COUNT(1) FROM NEXG_CDRS PARTITION("$datepart1");" 2>/dev/null)
S162=$(mysql -sN -h172.19.8.162 -uroot -p'Vp$9~ioKy6t^8&e' kannel -e "SELECT COUNT(1) FROM sent_sms PARTITION("$datepart1") WHERE momt='MT';" 2>/dev/null)

echo $datepart1"|Server 103 "$S103 "|Server 162 "$S162

echo "EVENT_DATE|SERVER_103|SERVER_162|DIFF" >mail.txt
echo $datepart1"|"$S103"|"$S162"|"$(($S103-$S162)) >>mail.txt

if [[ $S103 -eq $S162 ]]
then
mysql -h172.19.8.161 -uroot -p'Vp$9~ioKy6t^8&e' kannel -vvv -e "ALTER TABLE sent_sms DROP PARTITION $datepart1;" 2>/dev/null
java -jar SendMail.jar 2
else
java -jar SendMail.jar 2
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""
