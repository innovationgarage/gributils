#! /bin/sh

cd /data

while ! wget -O - "$ESURL" > /dev/null 2>&1; do sleep 1; done

gributils index --database="$ESURL" initialize

gributils server --database="$ESURL"
