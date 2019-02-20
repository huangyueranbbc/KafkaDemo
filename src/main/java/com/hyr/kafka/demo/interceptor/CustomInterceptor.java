package com.hyr.kafka.demo.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/*******************************************************************************
 * @date 2019-02-20 上午 11:12
 * @author: <a href=mailto:>黄跃然</a>
 * @Description:
 ******************************************************************************/
public class CustomInterceptor implements ProducerInterceptor<String, String> {
    private static final Logger log = LoggerFactory.getLogger(CustomInterceptor.class);

    private int successCount = 0;
    private int errorCount = 0;

    /**
     * 数据拦截处理
     *
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 只发送key为OK的消息
        if ("OK".equals(record.key())) {
            return record;
        } else {
            return null;
        }
    }

    /**
     * 当消息发送成功或失败时调用此方法
     *
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            errorCount++;
        } else {
            successCount++;
        }
    }

    @Override
    public void close() {
        log.info("producer close... error:{} success:{}", errorCount, successCount);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("get config:{}", configs);
    }
}
