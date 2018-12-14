#!/bin/bash -l
pdir=$(cd "`dirname $0`"; cd ../..;pwd)
cd "${pdir}"
java -Xms1536m -Xmx1536m -cp ".:lib/*" com.bigdata.dis.sdk.demo.consumer.AppConsumer
