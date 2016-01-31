/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mattring.demoflinkengines;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * @author Matthew
 */
public class EnginesMain {
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        ExecEngine execEngine = new ExecEngine(env);
        execEngine.run();
        
        RecoEngine recoEngine = new RecoEngine(env);
        recoEngine.run();
        
        env.execute();
    }
}
