#!/bin/bash

psql --command="DELETE FROM $6 a USING $6 b WHERE a.segmentid=b.segmentid AND a.hour=b.hour AND a.day=b.day AND a.date<b.date;" postgresql://$1:$2@$3:$4/$5







