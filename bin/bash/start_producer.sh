#!/bin/bash -l
pdir=$(cd "`dirname $0`"; cd ../..;pwd)
cd "${pdir}"
java -Xms2048m -Xmx2048m -cp ".:lib/*" com.bigdata.dis.sdk.demo.producer.AppProducer
