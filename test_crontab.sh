#! /bin/bash

DAY_KEY=$(date -d "$date -1 month" +%Y%m%d)
FIRST_DAY_PRE_MONTH=${DAY_KEY:0:6}01
LAST_DAY_PRE_MONTH=$(date -d "$FIRST_DAY_PRE_MONTH + 1 month - 1 day" +%Y%m%d)

echo 'DAY_KEY:'$DAY_KEY
echo $FIRST_DAY_PRE_MONTH
echo $LAST_DAY_PRE_MONTH

