#!/bin/bash

# compiles R stuff. 
# had problems with putting this into 
# maven antrun.
# param #1 -> R pkg version
# param #1 -> build version 

if [ $# -ne 2 ]; then 
  echo 'usage: buildR.sh <r-package> <maven-version>'
  exit 1
fi

pkgName=$1
mver=$2
d=`date +%F`

# strip -SNAPSHOT things that create problems 
rver=`sed 's/-SNAPSHOT\$/-00000000/' <<< $mver`

echo $ver

sed "s/^Version:/& ${rver}/" DESCRIPTION.tmpl | sed "s/^Date:/& ${d}/" | \
sed "s/^Package:/& ${pkgName}/" > ../src/main/Rpkg/DESCRIPTION 