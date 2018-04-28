@echo off

set sdk_version=1.3.0
set kafka_sdk_version=1.0.4
call mvn install:install-file -Dfile="dis-sdk-%sdk_version%.jar" -DgroupId=com.bigdata.dis -DartifactId=dis-sdk -Dversion=%sdk_version% -Dpackaging=jar -DpomFile="./dis-sdk-%sdk_version%.pom"

call mvn install:install-file -Dfile="dis-data-iface-%sdk_version%.jar" -DgroupId=com.bigdata.dis -DartifactId=dis-data-iface -Dversion=%sdk_version% -Dpackaging=jar -DpomFile="./dis-data-iface-%sdk_version%.pom"

call mvn install:install-file -Dfile="dis-kafka-adapter-%kafka_sdk_version%.jar" -DgroupId=com.bigdata.dis -DartifactId=dis-kafka-adapter -Dversion=%kafka_sdk_version% -Dpackaging=jar -DpomFile="./dis-kafka-adapter-%kafka_sdk_version%.pom"

call mvn install:install-file -Dfile="java-sdk-core-2.0.1.jar" -DgroupId=com.huawei.apigateway -DartifactId=java-sdk-core -Dversion=2.0.1 -Dpackaging=jar

pause