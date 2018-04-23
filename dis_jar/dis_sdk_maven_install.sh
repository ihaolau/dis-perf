#!/bin/bash
sdk_version=1.2.3
kafka_sdk_version=1.0.3
mvn install:install-file -Dfile="dis-sdk-${sdk_version}.jar" -DgroupId=com.bigdata.dis -DartifactId=dis-sdk -Dversion=${sdk_version} -Dpackaging=jar -DpomFile="./dis-sdk-${sdk_version}.pom"

mvn install:install-file -Dfile="dis-data-iface-${sdk_version}.jar" -DgroupId=com.bigdata.dis -DartifactId=dis-data-iface -Dversion=${sdk_version} -Dpackaging=jar -DpomFile="./dis-data-iface-${sdk_version}.pom"

mvn install:install-file -Dfile="dis-kafka-adapter-${kafka_sdk_version}.jar" -DgroupId=com.bigdata.dis -DartifactId=dis-kafka-adapter -Dversion=${kafka_sdk_version} -Dpackaging=jar -DpomFile="./dis-kafka-adapter-${kafka_sdk_version}.pom"

mvn install:install-file -Dfile="java-sdk-core-2.0.1.jar" -DgroupId=com.cloud.sdk -DartifactId=java-sdk-core -Dversion=2.0.1 -Dpackaging=jar