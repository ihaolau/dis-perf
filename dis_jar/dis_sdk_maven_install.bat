@echo off
set sdk_version=1.3.0-SNAPSHOT

rem call mvn install:install-file -Dfile=huaweicloud-sdk-java-dis-pom-%sdk_version%.pom -DgroupId=com.huaweicloud.dis -DartifactId=huaweicloud-sdk-java-dis-pom -Dversion=%sdk_version% -Dpackaging=pom

rem call mvn install:install-file -Dfile=huaweicloud-sdk-java-dis-%sdk_version%.jar -DgroupId=com.huaweicloud.dis -DartifactId=huaweicloud-sdk-java-dis -Dversion=%sdk_version% -Dpackaging=jar -DpomFile=huaweicloud-sdk-java-dis-%sdk_version%.pom

rem call mvn install:install-file -Dfile=huaweicloud-sdk-java-dis-iface-%sdk_version%.jar -DgroupId=com.huaweicloud.dis -DartifactId=huaweicloud-sdk-java-dis-iface -Dversion=%sdk_version% -Dpackaging=jar -DpomFile=huaweicloud-sdk-java-dis-iface-%sdk_version%.pom

rem call mvn install:install-file -Dfile=java-sdk-core-2.0.1.jar -DgroupId=com.huawei.apigateway -DartifactId=java-sdk-core -Dversion=2.0.1 -Dpackaging=jar

call mvn install:install-file -Dfile=dis-kafka-adapter-1.1.0.jar -DgroupId=com.bigdata.dis -DartifactId=dis-kafka-adapter -Dversion=1.1.0 -Dpackaging=jar -DpomFile=dis-kafka-adapter-1.1.0.pom
pause