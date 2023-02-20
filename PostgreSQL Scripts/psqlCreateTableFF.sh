#!/bin/bash

psql --command="CREATE TABLE $6(SEGMENTID TEXT, SPEED REAL, DATE TIMESTAMP);" postgresql://$1:$2@$3:$4/$5





