# Welcome to DIS Perf Tools


Getting Started
---

### Requirements

To get started using dis perf, you will need those things:

1. JRE 1.8 +
2. Can connect to dis endpoint
3. Maven 3+

### Package DIS Perf
use  maven command to package, and you will get dis-perf-X.X.X.zip in directory  `target`

```
mvn clean package
```

### Running DIS Perf with IDE
1. Copy `src\main\resources\dis.properties.templeate` to `src\main\resources\dis.properties`

2. Config dis.properties


| Name        | Description                              | Default                                  |
| :---------- | :--------------------------------------- | :--------------------------------------- |
| region      | Region in which the DIS is located. Currently, only the cnnorth-1 region is available for selection. | cn-north-1                               |
| ak          | User's plaintext AK. The My Credential page provides you the option to download your AK/SK file. | -                                        |
| sk          | User's plaintext SK. The My Credential page provides you the option to download your AK/SK file. | -                                        |
| projectId   | Project ID specific to your region. The My Credential page displays all ProjectIDs. | -                                        |
| endpoint    | DIS gateway address                      | https://dis.cn-north-1.myhuaweicloud.com |
| stream_name | You stream name which created in DIS     |                                          |


3. Start Producer, just Run `com.bigdata.dis.sdk.demo.producer.AppProducer`

4. Start Consumer, just Run `com.bigdata.dis.sdk.demo.consumer.AppConsumer`

### Running DIS Perf with package
1. You can download dis-perf-X.X.X.zip from target/
2. Unzip dis-perf-1.0.0.zip on Linux or Windows
```
unzip dis-perf-1.0.0.zip

or use winrar on Windows
```
3. Config dis.properties

   See **Running DIS Perf with IDE -> Config dis.properties**

4. Start Producer

```
dos2unix bin/bash/*.sh

bash bin/bash/start_producer.sh

or enter "bin" directory,and double click start_producer.bat on Windows
```

5. Start Consumer

```
dos2unix bin/bash/*.sh

bash bin/bash/start_consumer.sh

or enter "bin" directory,and double click start_consumer.bat on Windows
```

### Stopping DIS perf

```
Use Ctrl+C to Stop java program
```

### Logging Analysis

When you running producer, log will output per second. for example
```
TPS [44] / [43.14](9404/218), 	Throughput [440] / [431.38](94040/218), 	Latency [45] / [46.41](436448/9404), 	TotalRequestTimes [9406](success 9404 / failed 0), TotalSendRecords [94060](success 94040 / failed 0).
```
**TPS** :
 - 44 indicates how many requests are sent in the current second
 - 43.14 indicates the total average of TPS after running
 - 9404 indicates the total requests after running
 - 218 indicates the total seconds after running


**Throughput** :
 - 440 indicates how many records are sent in the current second
 - 431.38 indicates the total average of Throughput after running
 - 94040 indicates the total send records after running
 - 218 indicates the total seconds after running

**Latency** :
 - 45 indicates the average of latency in the current second
 - 46.41 indicates the total average of Latency after running
 - 436448 indicates the total latency after running
 - 9404 indicates the total requests after running


