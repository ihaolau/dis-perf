package com.bigdata.dis.sdk.demo.jzctest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.DISClientAsync;
import com.huaweicloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.request.GetRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.iface.data.response.GetRecordsResult;
import com.huaweicloud.dis.iface.data.response.Record;
import com.huaweicloud.dis.util.PartitionCursorTypeEnum;

public class LatencyTestSender {
	private static final Logger log = LoggerFactory.getLogger(LatencyTestSender.class);
	
	public static void main(String[] args) {
		DIS dis = new DISClientAsync(Constants.DIS_CONFIG);
		
		consumer(dis);
		
		send(dis);
	}

	private static void consumer(DIS dis) {
		GetPartitionCursorRequest getShardIteratorParam = new GetPartitionCursorRequest();
		getShardIteratorParam.setCursorType(PartitionCursorTypeEnum.LATEST.name());
		getShardIteratorParam.setStreamName(Constants.STREAM_NAME);
		getShardIteratorParam.setPartitionId(Integer.toString(0));
		
		new Thread() {
			public void run() {
				AtomicLong records = new AtomicLong();
				AtomicLong getCosts = new AtomicLong();
				AtomicLong writeCosts = new AtomicLong();
				AtomicLong records2CreateCosts = new AtomicLong();
				AtomicLong write2EndCosts = new AtomicLong();
				
				try {
					Thread.sleep(10000L);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				String cursor = dis.getPartitionCursor(getShardIteratorParam).getPartitionCursor();
				while(true) {
					GetRecordsRequest getRecordsParam = new GetRecordsRequest();
					getRecordsParam.setPartitionCursor(cursor);
					long start = System.currentTimeMillis();
					GetRecordsResult result = dis.getRecords(getRecordsParam);
					long end = System.currentTimeMillis();
			
					records.addAndGet(result.getRecords().size());
					for(Record record : result.getRecords()) {
						long recordCreate = Long.parseLong(record.getPartitionKey());
						long write = record.getTimestamp();
						
						getCosts.addAndGet(end-start);
						writeCosts.addAndGet(write-recordCreate);
						
						write2EndCosts.addAndGet(end-write);
						
						records2CreateCosts.addAndGet(end-recordCreate);
					}
					
					log.info("{} {}/{} , {} {} {}",result.getRecords().size(), end-start,getCosts.get()/records.get(), 
							writeCosts.get()/records.get(), write2EndCosts.get()/records.get(), records2CreateCosts.get()/records.get());
					
					cursor = result.getNextPartitionCursor();
				}
			};
		}.start();
		
		
	}

	private static void send(DIS dis) {
		for(int i=0; i<Constants.PRODUCER_THREAD_NUM; i++) {
			new Thread() {
				public void run() {
					while(true) {
						PutRecordsRequest putRecordsParam = new PutRecordsRequest();
						putRecordsParam.setStreamName(Constants.STREAM_NAME);
						
						List<PutRecordsRequestEntry> records = new ArrayList<>();
						for(int i=0; i<Constants.PRODUCER_REQUEST_RECORD_NUM; i++) {
							PutRecordsRequestEntry record = new PutRecordsRequestEntry();
							record.setData(ByteBuffer.wrap(new byte[Constants.PRODUCER_RECORD_LENGTH]));
							record.setPartitionId(Integer.toString(0));
							record.setPartitionKey(Long.toString(System.currentTimeMillis()));
							records.add(record);
						}
						
						putRecordsParam.setRecords(records);
						
						dis.putRecords(putRecordsParam);
//						System.out.println("haha");
					}
				};
			}.start();
		}
		
	}
}
