#!/bin/bash

# select parallel run logs in the past 7 days (default)
# you can use -d <past_days> to specify past days 

LOG_PATH=~/import/*/rundata/
PAST_DAYS=7
REPORT_DATE=`date "+%Y%m%d"`
OUTPUT_DIR=~/import/parallel_log_analysis

while getopts "d:" option
do
 case "${option}"
 in
 d) PAST_DAYS=${OPTARG} ;;
 \?)
    echo "Invalid option: -$OPTARG" >&2
    exit
    ;;
 esac
done

echo $PAST_DAYS

OUTPUT_FILE=${OUTPUT_DIR}/${REPORT_DATE}-${PAST_DAYS}_paired_entries.txt

# find log files created in the past 7 days and get the matched lines
find $LOG_PATH -mtime -${PAST_DAYS} -type f -exec grep "create_cid_field : new" -A1 {} \+ | sed "/--$/d" | sed "s/.txt-/.txt:/g" > ${OUTPUT_FILE}

PIPFILE="/apps/htmm/zephir-services/Pipfile"
SCRIPT="/apps/htmm/zephir-services/scripts/check_cid_difference.py"

if [ ! -e $SCRIPT ]; then
  usage_error "Script $SCRIPT does not exist."
fi

cmd="pipenv run python $SCRIPT $OUTPUT_FILE"

PIPENV_PIPFILE=$PIPFILE $cmd
