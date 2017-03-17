dataSyncclient=$(ps -ef | grep com.yqzk.markRepeat.main.DumperCustoms | grep -v grep | wc -l)
echo $dataSyncclient
if [ "$dataSyncclient" -eq 0 ]
 then 
 {
echo "------------------------------------------------------"
export JAVA_HOME=/opt/jdk1.6.0_43
export PRE_HOME=/opt/project/markRepeat
echo "PRE_HOME="$PRE_HOME

export CLASSPATH=$PRE_HOME/conf
export CLASSPATH="$CLASSPATH":$PRE_HOME/bin

LIBS_DIR=$PRE_HOME/lib
for i in $LIBS_DIR/*.jar
do CLASSPATH="$CLASSPATH":$i
done
echo "CLASSPATH="$CLASSPATH

#out java version
$JAVA_HOME/bin/java -version

#set jvm arg
export JVM_ARG="-Xms512M -Xmx1024M"
echo "JVM_ARG="$JVM_ARG
export LANG="zh_CN.UTF-8"
echo "------------------------------------------------------"

if [ $# > 0 ] ;
then
{
    if [ "$1" = "1" ] ; then
   {
        echo "scan hbase all data";
    }
     else
        echo "scan hbase where data is repeated";
     fi
}
 else
    echo "scan hbase where data is repeated"
fi

echo "$*"

$JAVA_HOME/bin/java $JVM_ARG com.yqzk.markRepeat.main.DumperCustoms $* > /opt/project/markRepeat/logs/dumper.log & \

echo "system started in RUNNING mode!"
}
 else
   echo  "exit"
fi
