
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
public class RecoEngine extends EnvAware<StreamExecutionEnvironment> implements Runnable {
    
    public RecoEngine(StreamExecutionEnvironment env) {
        super(env);
    }

    @Override
    public void run() {
        NatsConfig sourceConfig= new NatsConfig("nats://localhost:4222", "ticks", 3, 1000);
        SourceFunction<String> tickSourceFunction = new NatsSource(sourceConfig, null);
        NatsConfig sinkConfig= new NatsConfig("nats://localhost:4222", "recos", 3, 1000);        
        SinkFunction<String> recoSinkFunction = new NatsSink(sinkConfig);
        
        DataStreamSource<String> tickStream = env.addSource(tickSourceFunction);
        
        tickStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String tick) throws Exception {
                
                String recoOut = null;
                
                if (tick != null) {
                    System.out.println(tick);
                    String recoType = null;
                    final double typeDecision = Math.random();
                    if (typeDecision < 0.10d) {
                        recoType = "RB";
                    } else if (typeDecision > 0.90d) {
                        recoType = "RS";
                    }

                    if (recoType != null) {
                        // tick: T|sym|last|exchange
                        final String[] tickParts = tick.split("\\|");
                        final String sym = tickParts[1];
                        final double recoPrice = Double.parseDouble(tickParts[2]);
                        // reco: [RB or RS]|Sym|recoPrice
                        recoOut = String.format("%s|%s|%.2f", recoType, sym, recoPrice);
                    }
                }
                
                return recoOut;
        }}).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String reco) throws Exception {
                return reco != null;
            }
        }).addSink(recoSinkFunction);
        
        
    }
    
}
