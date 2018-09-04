#!/bin/bash -l
pdir=$(cd "`dirname $0`"; cd ../..;pwd)
cd "${pdir}"
java -Xmx512m -cp ".:lib/*" com.bigdata.dis.sdk.demo.producer.mqtt.AppMqttProducer
