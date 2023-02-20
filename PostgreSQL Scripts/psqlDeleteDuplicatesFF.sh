#!/bin/bash

psql --command="DELETE FROM $6 a USING $6 b WHERE a.segmentid=b.segmentid AND a.date<b.date;" postgresql://$1:$2@$3:$4/$5







