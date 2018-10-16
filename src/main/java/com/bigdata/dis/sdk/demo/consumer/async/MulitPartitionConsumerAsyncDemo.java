package com.bigdata.dis.sdk.demo.consumer.async;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DISAsync;
import com.huaweicloud.dis.DISClientAsync;
import com.huaweicloud.dis.DISClientAsync2;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.request.GetRecordsRequest;
import com.huaweicloud.dis.iface.data.response.GetRecordsResult;
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import com.huaweicloud.dis.util.PartitionCursorTypeEnum;

public class MulitPartitionConsumerAsyncDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(MulitPartitionConsumerAsyncDemo.class);

    static int THREAD_COUNT = 2;

    static ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

    static volatile boolean isStop = false;

    static final String STREAM_NAME = Constants.STREAM_NAME;

    public static void main(String[] args) {
        for (int m = 0; m < THREAD_COUNT; m++) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        MulitPartitionConsumerAsyncDemo.run();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            executorService.submit(th);
        }
    }

    private static class CursorDes{
    	int partitionId;
    	String cursor;
    	public CursorDes(int partitionId, String cursor) {
    		this.partitionId = partitionId;
    		this.cursor = cursor;
    	}
    }
    
    static public void run()
            throws InterruptedException {
        final String threadName = Thread.currentThread().getName();

        boolean isNIOAsync = "true".equals(Constants.DIS_CONFIG.get("NIOAsync"));
        final DISAsync dis = isNIOAsync ? new DISClientAsync2(Constants.DIS_CONFIG) : new DISClientAsync(Constants.DIS_CONFIG, Executors.newFixedThreadPool(10));

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(Constants.STREAM_NAME);
		DescribeStreamResult streamRes = dis.describeStream(describeStreamRequest);
		int parCount = streamRes.getPartitions().size();
		
		ArrayBlockingQueue<CursorDes> toGets = new ArrayBlockingQueue<>(parCount);
		for(int i=0; i<parCount; i++) {
			GetPartitionCursorRequest getShardIteratorParam = new GetPartitionCursorRequest();
			getShardIteratorParam.setCursorType(PartitionCursorTypeEnum.TRIM_HORIZON.name());
			getShardIteratorParam.setStreamName(Constants.STREAM_NAME);
			getShardIteratorParam.setPartitionId(Integer.toString(i));
			
			toGets.put(new CursorDes(i, dis.getPartitionCursor(getShardIteratorParam).getPartitionCursor()));
		}
		
        Map<String, Integer> resultCount = new HashMap<>();
        final AtomicInteger totalCount = new AtomicInteger();
		
		while(!isStop) {
			CursorDes tmp = toGets.take();
			
			GetRecordsRequest getRecordsParam = new GetRecordsRequest();
			getRecordsParam.setPartitionCursor(tmp.cursor);
			
			final int flag = tmp.partitionId;
			
			final long startMs = System.currentTimeMillis();
			dis.getRecordsAsync(getRecordsParam, new AsyncHandler<GetRecordsResult>() {
				@Override
				public void onSuccess(GetRecordsResult getRecordsResult) {

                    try {
                        LOGGER.info("!{} Partition {} get {} records, cost {}ms, startSeq {}", threadName,
                                flag,
                                getRecordsResult.getRecords().size(), (System.currentTimeMillis() - startMs),
                                getRecordsResult.getRecords().get(0).getSequenceNumber());
                        if (getRecordsResult.getRecords().size() > 0) {
                        	totalCount.addAndGet(getRecordsResult.getRecords().size());
//                            for (Record record : getRecordsResult.getRecords()) {
//                                totalCount.incrementAndGet();
//                        String content =
//                            new String(record.getData().array(), "utf-8").replaceAll("\r", "\\\\r")
//                                .replaceAll("\n", "\\\\n");
//                        resultCount.merge(content, 1, (a, b) -> a + b);
//                        LOGGER.info(
//                            "Record[Content={}, PartitionKey={}, SequenceNumber={}], APPEND INFO[{}]",
//                            new String(record.getData().array(), "utf-8").replaceAll("\r", "\\\\r")
//                                .replaceAll("\n", "\\\\n"),
//                            record.getPartitionKey(),
//                            record.getSequenceNumber());
//                            }
                        } else {
                            LOGGER.info("Response size 0");
                            for (String s : resultCount.keySet()) {
                                LOGGER.info(resultCount.get(s) + " | " + s);
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage());
                    }
                
					
//					LOGGER.info("{}, {}", result.getRecords().size(), System.currentTimeMillis() - start);
					try {
						toGets.put(new CursorDes(flag, getRecordsResult.getNextPartitionCursor()));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				@Override
				public void onError(Exception exception) {
                    String msg = exception.getMessage();
                    if(!msg.contains("Exceeded traffic control limit"))
                    {
                        isStop = true;
                    }
                    LOGGER.error("!{} Cost " + (System.currentTimeMillis() - startMs) + "ms, " + exception.getMessage(), threadName, exception);
                }
			});
		}
       
    }
}