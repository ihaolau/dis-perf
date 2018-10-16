package com.bigdata.dis.sdk.demo.jzctest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.DISAsync;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.DISClientAsync;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.request.GetRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.iface.data.response.GetRecordsResult;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import com.huaweicloud.dis.util.PartitionCursorTypeEnum;

public class JzcTest {
	private static final Logger log = LoggerFactory.getLogger(JzcTest.class);
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
//		testProducer();
		testConsumer1();
//		testConsumer2();
	}

	private static void testConsumer2() {
		DIS dis = new DISClientAsync(Constants.DIS_CONFIG);
		GetPartitionCursorRequest getShardIteratorParam = new GetPartitionCursorRequest();
		getShardIteratorParam.setCursorType(PartitionCursorTypeEnum.TRIM_HORIZON.name());
		getShardIteratorParam.setStreamName(Constants.STREAM_NAME);
		getShardIteratorParam.setPartitionId(Integer.toString(0));
		
		String cursor = dis.getPartitionCursor(getShardIteratorParam).getPartitionCursor();
		while(true) {
			GetRecordsRequest getRecordsParam = new GetRecordsRequest();
			getRecordsParam.setPartitionCursor(cursor);
			long start = System.currentTimeMillis();
			GetRecordsResult result = dis.getRecords(getRecordsParam);
			log.info("{}, {}", result.getRecords().size(), System.currentTimeMillis() - start);
			cursor = result.getNextPartitionCursor();
		}
	}
	
	private static void testConsumer1() throws InterruptedException {
		DISAsync disA = new DISClientAsync(Constants.DIS_CONFIG);
		
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(Constants.STREAM_NAME);
		DescribeStreamResult streamRes = disA.describeStream(describeStreamRequest);
		int parCount = streamRes.getPartitions().size();
		
		ArrayBlockingQueue<String> s = new ArrayBlockingQueue<>(2*parCount);
		for(int i=0; i<parCount; i++) {
			GetPartitionCursorRequest getShardIteratorParam = new GetPartitionCursorRequest();
			getShardIteratorParam.setCursorType(PartitionCursorTypeEnum.TRIM_HORIZON.name());
			getShardIteratorParam.setStreamName(Constants.STREAM_NAME);
			getShardIteratorParam.setPartitionId(Integer.toString(i));
			
			s.put(disA.getPartitionCursor(getShardIteratorParam).getPartitionCursor());
		}
		
		
		String tmp;
		while((tmp = s.take()) != null) {
			GetRecordsRequest getRecordsParam = new GetRecordsRequest();
			getRecordsParam.setPartitionCursor(tmp);
			
			final long start = System.currentTimeMillis();
			disA.getRecordsAsync(getRecordsParam, new AsyncHandler<GetRecordsResult>() {
				@Override
				public void onSuccess(GetRecordsResult result) {
					log.info("{}, {}", result.getRecords().size(), System.currentTimeMillis() - start);
					try {
						s.put(result.getNextPartitionCursor());
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				@Override
				public void onError(Exception exception) {
					exception.printStackTrace();
				}
			});
		}
		
		
	}

	private static void testProducer() throws InterruptedException, ExecutionException {
		DISAsync disA = new DISClientAsync(Constants.DIS_CONFIG);
		
		PutRecordsRequest putRecordsParam = new PutRecordsRequest();
		putRecordsParam.setStreamName(Constants.STREAM_NAME);
		List<PutRecordsRequestEntry> records = new ArrayList<>();
		for(int i=0; i<Constants.PRODUCER_REQUEST_RECORD_NUM; i++) {
			PutRecordsRequestEntry record = new PutRecordsRequestEntry();
			record.setData(ByteBuffer.wrap(new byte[Constants.PRODUCER_RECORD_LENGTH]));
			records.add(record);
		}
		
		putRecordsParam.setRecords(records);
		
		send(disA, putRecordsParam);
		
//		for(int i=0; i<1; i++) {
//			new Thread() {
//				@Override
//				public void run() {
//					send(disA, putRecordsParam);
//				}
//			}.start();
//		}
		
		Thread.sleep(100000000L);
	}
	
	private static void send(DISAsync disA, PutRecordsRequest putRecordsParam) {
		while(true) {
			Future<PutRecordsResult> future = disA.putRecordsAsync(putRecordsParam, new AsyncHandler<PutRecordsResult>() {
				
				@Override
				public void onSuccess(PutRecordsResult result) {
					// TODO Auto-generated method stub
//					System.out.println("heihei");
				}
				
				@Override
				public void onError(Exception exception) {
					exception.printStackTrace();
				}
			});
		}
	}
	
}
