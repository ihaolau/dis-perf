#!/bin/bash -l
pdir=$(cd "`dirname $0`"; cd ../..;pwd)
cd "${pdir}"
java -Xms512m -Xmx512m -cp ".:lib/*" com.bigdata.dis.sdk.demo.other.loggen.LogGen0 $#
