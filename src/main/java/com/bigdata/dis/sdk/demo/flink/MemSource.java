package com.bigdata.dis.sdk.demo.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;  
import org.apache.flink.streaming.api.functions.source.SourceFunction;  
  
public class MemSource implements SourceFunction<String> {  
  
    /** 
     * 产生数据 
     */  
    @Override  
    public void run(SourceContext<String> sourceContext) throws Exception {  
        while (true) {  
            sourceContext.collect("flink spark storm");  
        }  
    }  
  
    /** 
     * 关闭资源 
     */  
    @Override  
    public void cancel() {  
    }  
  
}  
  
class RSource extends RichSourceFunction<String> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceFunction.SourceContext<String> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
  