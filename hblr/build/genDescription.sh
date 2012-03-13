#!/bin/bash

# compiles R stuff. 
# had problems with putting this into 
# maven antrun.
# param #1 -> R pkg version
# param #2 -> build version 
# param #3 -> output file name

if [ $# -ne 3 ]; then 
  echo 'usage: genDescription.sh <r-package> <maven-version> <output-DESCRIPTION-file-path>'
  exit 1
fi

pkgName=$1
mver=$2
d=`date +%F`

# strip -SNAPSHOT things that create problems 
rver=`sed 's/-SNAPSHOT\$/-00000000/' <<< $mver`

echo $ver

sed "s/^Version:/& ${rver}/" DESCRIPTION.tmpl | sed "s/^Date:/& ${d}/" | \
sed "s/^Package:/& ${pkgName}/" > $3 