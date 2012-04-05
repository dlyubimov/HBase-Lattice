#!/bin/bash

# compiles R stuff. 
# had problems with putting this into 
# maven antrun.
# param #1 -> R pkg version
# param #2 -> build version 
# param #3 -> output pkgdir

if [ $# -ne 4 ]; then 
  echo 'usage: genDescription.sh <r-package> <maven-version> <R pkg input dir> <output-generated-sources-path>'
  exit 1
fi

test ! -f "`which R`" && { echo "Cannot find R. is R installed? In the path?"; exit 1; }

pkgName=$1
mver=$2
pkgdir=$3
outdir=$4
descrFile=$outdir/DESCRIPTION

d=`date +%F`

h=`date +%Y%m%d%H%M%S`

# strip -SNAPSHOT things that create problems 
rver=`sed "s/-SNAPSHOT\$/-$h/" <<< $mver`

echo $ver

sed "s/^Version:/& ${rver}/" DESCRIPTION.tmpl | sed "s/^Date:/& ${d}/" | \
sed "s/^Package:/& ${pkgName}/" > "$descrFile" 

# also while we are at it, try to generate manuals using roxygen2 package
# and copy the entire thing 

R --vanilla <<EOF

# copy stuff
files2copy <- list.files("${pkgdir}",full.names=T, include.dirs=T) 
file.copy(from=files2copy,to="${outdir}", recursive=T)

# gen docs 
library("roxygen2")
roxygenize(package.dir="${outdir}")

EOF
