# compile AND install R package. 
# need to have R, rJava  and have a sudo 
# for this to work. Also have maven executable around

MVN='mvn clean install -DskipTests -DR'

sudo R CMD REMOVE hblr; { $MVN && sudo HADOOP_HOME=$HADOOP_HOME HBASE_HOME=$HBASE_HOME R_COMPILE_PKGS=1 R CMD INSTALL --build target/hblr-0.2.0-SNAPSHOT-rpkg; }
