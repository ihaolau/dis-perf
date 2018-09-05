#!/bin/bash -l
pdir=$(cd "`dirname $0`"; cd ../..;pwd)
cd "${pdir}"
java -Xmx2048m -cp ".:lib/*" com.bigdata.dis.sdk.demo.consumer.AppConsumer
