package com.hyr.kafka.demo.streams.low.dsl.process;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/*******************************************************************************
 * @date 2017-12-26 下午 2:10
 * @author: <a href=mailto:>黄跃然</a>
 * @Description: 自定义Processor
 ******************************************************************************/
public class MyProcessor implements Processor<String, String> {
    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    public void init(ProcessorContext context) {
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context;

        // call this processor's punctuate() method every 1000 milliseconds.
        this.context.schedule(1000); // 执行punctuate()的周期

        // retrieve the key-value store named "Counts"
        this.kvStore = (KeyValueStore<String, Long>) context.getStateStore("Counts"); // 获取"Counts"状态
    }

    // 处理步骤
    public void process(String dummy, String line) {
        String[] words = line.toLowerCase().split(" "); // 分词

        // 统计单词出现次数 word count
        for (String word : words) {
            // 将统计结果放入存储状态State存储 Put the statistics into the storage state State storage
            Long oldValue = this.kvStore.get(word);

            if (oldValue == null) {
                this.kvStore.put(word, 1L);
            } else {
                this.kvStore.put(word, oldValue + 1L);
            }
        }
    }

    // 将数据发送到下游处理器，提交当前流的状态 Send the data to the downstream processor and submit the current flow status
    public void punctuate(long timestamp) {
        KeyValueIterator<String, Long> iter = this.kvStore.all();

        while (iter.hasNext()) {
            KeyValue<String, Long> entry = iter.next();
            System.out.println("key" + entry.key + "\t value:" + entry.value.toString());
            context.forward(entry.key, "key" + entry.key + "\t value:" + entry.value.toString()); // 相当于context.emit() 参考storm
        }

        iter.close();
        // commit the current processing progress
        context.commit();
    }

    public void close() {
        // close any resources managed by this processor.
        // Note: Do not close any StateStores as these are managed
        // by the library
    }


}
