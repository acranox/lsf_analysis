#!/bin/bash

USAGE="Usage: ${0##*/} [options] 

Options:
    -y year
    -o output directory 
    -c path to my.conf file
"

YEAR=$(date +%Y)
OUTDIR=./
MYCONF="./.my.conf"

# Getopts loop
while getopts ":y:o:c:" OPTION
do
  case $OPTION in
    y) export YEAR="$OPTARG";;
    o) export OUTDIR="$OPTARG";;
    c) export MYCONF="$OPTARG";;
    ?) echo "Option -$OPTARG requires an argument." >&2
       exit 1;;
    :) echo "Option -$OPTARG requires an argument." >&2
       exit 1;;
    *) echo "$USAGE" >&2
       exit 1;;
  esac
done
shift $(($OPTIND - 1))

# Exit if we have more than one variable
if [ -n "$2" ];then
    echo "$USAGE" >&2
    exit 1
fi

if [[ ! -d $OUTDIR ]]; then
    mkdir $OUTDIR
fi

if [[ ! -e $MYCONF ]]; then
    echo "$MYCONF does not exist" >&2
    echo "$USAGE" >&2
    exit 1
fi

# Loop through the months 

function rtm_query
{
MYSQL="mysql --defaults-file=$MYCONF -e"
for COUNT in {1..12};
do
    COUNTPLUS=$(($COUNT+1))
    PADCOUNT=$(printf "%02d" $COUNT)
    QUERY="SELECT jobid,user,stat_changes,exitStatus,queue,mem_used,mem_reserved,cpu_used,efficiency,num_cpus,pend_time,psusp_time,run_time,ususp_time,ssusp_time,unkwn_time,CASE WHEN dependCond = '' THEN 'nojobdepend' ELSE 'jobdepend' END FROM cacti.grid_jobs_finished WHERE end_time >= '$YEAR-"${COUNT}"-1 00:00:00' AND end_time < '$YEAR-"${COUNTPLUS}"-1 00:00:00' INTO OUTFILE '/tmp/${PADCOUNT}-$YEAR.tsv' ;"

    echo $QUERY
    eval $MYSQL \"$QUERY\"
    if [[ -s /tmp/${PADCOUNT}-$YEAR.tsv ]]; then
        cp -v /tmp/${PADCOUNT}-$YEAR.tsv $OUTDIR
    fi

done
}

rtm_query
