package com.hyr.kafka.demo.streams.low.dsl;

import com.hyr.kafka.demo.streams.low.dsl.process.MyProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;

/*******************************************************************************
 * @date 2017-12-26 下午 2:10
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: 低级Stream DSL接口TopologyBuilder WordCount
 ******************************************************************************/
public class LowDSLMain {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 指定K-V 格式
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsConfig config = new StreamsConfig(props);

        TopologyBuilder builder = new TopologyBuilder(); // 低级Stream DSL接口

        // 自定义的处理器Processor Custom processor
        ProcessorSupplier processor1 = new ProcessorSupplier() {
            public Processor get() {
                return new MyProcessor();
            }
        };
        builder.addSource("SOURCE", "my-input-topic")
                .addProcessor("PROCESS1", processor1, "SOURCE") // 添加处理器到处理器拓扑中 Add the processor to the processor topology
                // 设置sink处理器,没有下游处理器。将上游数据发送到指定的topic中。 Set the sink processor without a downstream processor. Send the upstream data to the specified topic.
                .addSink("SINK1", "my-output-topic", "PROCESS1");


        // 本地状态存储,可以用于全局监控和统计 Local state storage, which can be used for global monitoring and statistics
        StateStoreSupplier countStore = Stores.create("Counts")
                .withKeys(Serdes.String())
                .withValues(Serdes.Long())
                //.persistent() // 持久化到磁盘 会读取到过去到现在所有持久化的数据
                .inMemory() // 将存储状态state存储在内存
                .build();

        builder.addStateStore(countStore, "PROCESS1");  // 对指定的processor处理器添加存储状态 Add storage status to the specified processor processor

        // 启动
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }

}
