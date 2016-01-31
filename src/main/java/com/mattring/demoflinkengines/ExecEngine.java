package com.mattring.demoflinkengines;

import com.mattring.flink.nats.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 *
 * @author mring
 */
public class ExecEngine extends EnvAware<StreamExecutionEnvironment> implements Runnable {

    public ExecEngine(StreamExecutionEnvironment env) {
        super(env);
    }

    @Override
    public void run() {
        NatsConfig ordersSourceConfig = new NatsConfig("nats://localhost:4222", "orders", 3, 1000);
        SourceFunction<String> ordersSourceFunction = new NatsSource(ordersSourceConfig, null);
        NatsConfig execSinkConfig= new NatsConfig("nats://localhost:4222", "execs", 3, 1000);        
        SinkFunction<String> execSinkFunction = new NatsSink(execSinkConfig);
        
        DataStreamSource<String> orderStream = env.addSource(ordersSourceFunction);
        
        orderStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String order) throws Exception {
                
                String execOut = null;
                
                if (order != null) {
                    System.out.println(order);
                    String execType = order.startsWith("OB") ? "EB" : "ES";
                    
                    // order: [OB or OS]|Sym|Trader|recoPrice
                    final String[] orderParts = order.split("\\|");
                    final String sym = orderParts[1];
                    final String trader = orderParts[2];
                    final String recoPrice = orderParts[3];
                    // exec: [EB or ES]|Sym|Trader|Price
                    execOut = String.format("%s|%s|%s|%s", execType, sym, trader, recoPrice);
                }
                
                return execOut;
        }}).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String exec) throws Exception {
                return exec != null;
            }
        }).addSink(execSinkFunction);

    }

}
