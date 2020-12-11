#!/bin/bash
#
# Cleans up temporary files produced by the cid_inquiry process
# Script: prune_cid_inquiry_tmp_files.sh
#

FILE_PATH=/apps/htmm/htprep-data/cid-inquiry/done

# remove files that are older than 3 days
find $FILE_PATH -mtime +3 -type f -exec rm -r {} \;



