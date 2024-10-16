#!/bin/bash
satrt1=$(date)
echo $satrt1 StartDateTime
echo ""

day1=$(date --date="-0 days" +%Y_%m_%d)

export path1=/data01/shfiles/ra_reports/file_cnt_reco
export path2=/backup/infobip/${day1}
export path3=/data01/shfiles/infobip_loading/reco

kill1=$(ps -eo comm,pid,etimes,cmd | grep file_cnt_reco.sh | awk '{if($3 > 1800) print $2}')

if [ -z "$kill1" ]
then
echo "NO LONG RUNNING PROCESS FOUND TO BE KILL"
else
echo "LONG PROCESS KILLED"
kill -9 $kill1
fi

cd ${path1}
rm -f ${path1}/*.txt

awk -F "|" 'NR==FNR{s=$2;a[s]=$0; next} a[$1]{print $0"|"a[$1]}' <(cat ${path3}/CDRs_file_row_count_*.txt | awk -F"|" '{gsub(/.csv/,"",$2); print $1"|"$2"|"$3"|""db"}') <(gunzip -c ${path2}/NexG*.zip | awk -F"," '{gsub(/.cdr/,"",$1); print $1"|"($4-1)"|""infobip"}') | awk -F"|" '{if(($2-$6)!="0") print $1"|"$2"|"$4"|"$6"|"$2-$6}' >${path1}/cnt_reco.txt

awk -F "|" '
    FNR == NR {
        data[ ($2) ] = 1;
        next;
    }
    FNR < NR {
        if ( ! ($1 in data) ) {
            print $0;
        }
    }
' <(cat ${path3}/CDRs_file_row_count_*.txt | awk -F"|" '{gsub(/.csv/,"",$2); print $1"|"$2"|"$3"|""db"}') <(gunzip -c ${path2}/NexG*.zip | awk -F"," '{gsub(/.cdr/,"",$1); print $1"|"$2"|""infobip"}') >>${path1}/cnt_reco.txt

echo "FILE_NAME|INFOBIP_FILE_COUNT|CDR_LOAD_DATE|DB_FILE_COUNT|DIFFERENCES" >${path1}/file_cnt.txt
awk -F "|" 'NR==FNR{s=$2;a[s]=$0; next} a[$1]{print $0"|"a[$1]}' <(cat ${path3}/CDRs_file_row_count_*.txt | awk -F"|" '{gsub(/.csv/,"",$2); print $1"|"$2"|"$3"|""db"}') <(gunzip -c ${path2}/NexG*.zip | awk -F"," '{gsub(/.cdr/,"",$1); print $1"|"($4-1)"|""infobip"}') | awk -F"|" '{print $1"|"$2"|"$4"|"$6"|"$2-$6}' >>${path1}/file_cnt.txt

cat file_cnt.txt | awk -F"|" '{print "INFOBIP""|"$2"|"$5}' | awk -F "|" '{a[$1]++;b[$1]+=$2;c[$1]+=$3} END {for (i in a){printf "%s|%0.0d|%0.0d|%0.0d\n",i,a[i],b[i],c[i]}}' | awk -F"|" '{if($4=="") print $1"|"$2"|"$3"|""0"} {if($4!="") print $1"|"$2"|"$3"|"$4}' >>${path1}/mail1.txt


find ${path1}/*.txt -type f -size 0 -exec rm -rvf {} \;

if [ -f ${path1}/mail1.txt ]
then
echo "PLATFORM|FILE_COUNT|CDRS_COUNT" >${path1}/mail.txt
cat mail1.txt >>${path1}/mail.txt
java -jar SendMail.jar 1
fi

if [ -f ${path1}/cnt_reco.txt ]
then
echo "FILE_NAME|INFOBIP_FILE_COUNT|CDR_LOAD_DATE|DB_FILE_COUNT|DIFFERENCES" >${path1}/cnt_reco1.txt
cat cnt_reco.txt >>${path1}/cnt_reco1.txt
java -jar SendMail.jar 2
fi

echo ""
end1=$(date)
echo $end1 EndDateTime
timediff1=$(printf "%s\n" $(( $(date -d "$end1" "+%s") - $(date -d "$satrt1" "+%s") )))
echo $timediff1 Sec
echo ""

