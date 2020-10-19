#!/bin/bash
FILES=  datafordeleren/*.gml
echo FILES
for f in $FILES
do
  file=`basename "$f"`
  echo ${file%.*} 
  echo $f
  # take action on each file. $f store current file name
  ogr2ogr -f "GeoJSON" datafordeleren/${file%.*}.json $f 
done
