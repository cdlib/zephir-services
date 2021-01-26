#!/bin/bash

# select parallel run logs in the past 7 days (default)
# you can use -p <past_days> to specify past days 
# Options:
# -p <past_days>: optional. Default is 7 which selects run reports created in the past 7 days.
# -f <input_file>: optional. Process the specified when this option is used. 
# -d <input_dir>: optional. Process files in the specidied directory when this option is used.
# -r <report_id>: optional. Add report DI to the report filename when specified. 

function usage {
    echo "Usage: select_parallel_log.sh -p <past_days> -f <input_file> -d <input_dir> -r <report_id>"
    echo "Options:"
    echo " -p <past_days>: optional. Default is 7 which selects run reports created in the past 7 days."
    echo " -f <input_file>: optional. Process the specified when this option is used." 
    echo " -d <input_dir>: optional. Process files in the specidied directory when this option is used."
    echo " -r <report_id>: optional. Add report DI to the report filename when specified."
}

LOG_PATH=~/import/*/rundata/
PAST_DAYS=7
REPORT_DATE=`date "+%Y%m%d"`
OUTPUT_DIR=~/import/parallel_log_analysis
LOG_FILE=""
LOG_DIR=""

while getopts "d:f:p:r:h" option
do
 case "${option}"
 in
 p) PAST_DAYS=${OPTARG} ;;
 f) LOG_FILE=${OPTARG} ;;
 d) LOG_DIR=${OPTARG} ;;
 r) RPT_ID=${OPTARG} ;;
 h) usage
    exit 
    ;;
 \?)
    echo "Invalid option: -$OPTARG" >&2
    usage
    exit
    ;;
 esac
done

#echo $PAST_DAYS
#echo $LOG_FILE

if [[ -f "$LOG_FILE" ]]; then
  filename=`basename $LOG_FILE`
  OUTPUT_FILE=${OUTPUT_DIR}/${REPORT_DATE}_${filename}_paired_entries.txt
  #echo $OUTPUT_FILE
  grep "create_cid_field" -A1 $LOG_FILE | grep -v -E "INFO|WARNING|Success" > ${OUTPUT_FILE}
elif [[ -d "$LOG_DIR" ]]; then
  if [ -z "$RPT_ID" ]; then
    RPT_ID=`basename $LOG_DIR`
  fi
  #echo $RPT_ID
  OUTPUT_FILE=${OUTPUT_DIR}/${REPORT_DATE}_${RPT_ID}_paired_entries.txt
  #echo $OUTPUT_FILE
  grep "create_cid_field" -A1 $LOG_DIR/*.txt | grep -v -E "INFO|WARNING|Success" | sed "/--$/d" | sed "s/.txt-/.txt:/g" > ${OUTPUT_FILE}
else
  OUTPUT_FILE=${OUTPUT_DIR}/${REPORT_DATE}-${PAST_DAYS}_paired_entries.txt
  # find log files created in the past 7 days and get the matched lines
  find $LOG_PATH -mtime -${PAST_DAYS} -type f -exec grep "create_cid_field" -A1 {} \+ | grep -v -E "INFO|WARNING|Success" | sed "/--$/d" | sed "s/.txt-/.txt:/g" > ${OUTPUT_FILE}
fi

PIPFILE="/apps/htmm/zephir-services/Pipfile"
SCRIPT="/apps/htmm/zephir-services/scripts/check_cid_difference.py"

if [ ! -e $SCRIPT ]; then
  usage_error "Script $SCRIPT does not exist."
fi

cmd="pipenv run python $SCRIPT $OUTPUT_FILE"

PIPENV_PIPFILE=$PIPFILE $cmd
