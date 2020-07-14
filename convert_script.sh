#!/bin/bash
FILES=/home/mehigh/geo/geogml/*.gml
for f in $FILES
do
  file=`basename "$f"`
  echo ${file%.*} 
  echo $f
  # take action on each file. $f store current file name
  ogr2ogr -f "GeoJSON" /home/mehigh/geo/${file%.*}.json $f 
done
