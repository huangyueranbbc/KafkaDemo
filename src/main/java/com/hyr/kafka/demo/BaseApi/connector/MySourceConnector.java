package com.hyr.kafka.demo.BaseApi.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.*;

/*******************************************************************************
 * @date 2017-12-27 上午 9:59
 * @author: <a href=mailto:>黄跃然</a>
 * @Description: 自定义的连接器Connector SourceConnectors从其他系统导入数据 SinkConnector导出数据
 ******************************************************************************/
public class MySourceConnector extends SourceConnector {

    public static final String TOPIC_CONFIG = "topic";
    public static final String FILE_CONFIG = "file";

    private String filename;
    private String topic;

    // ConfigDef类用于指定预期的配置集，对于每个配置，你可以指定name，type，默认值，描述，group信息，group中的顺序，配置值的宽和适于在UI显示的名称。
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FILE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Source filename.")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The topic to publish data to");

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    // 生命周期方法 从配置文件中加载相关属性
    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FILE_CONFIG);
        topic = props.get(TOPIC_CONFIG);
        if (topic == null || topic.isEmpty())
            throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
        if (topic.contains(","))
            throw new ConnectException("FileStreamSourceConnector should only have a single topic when used as a source.");
    }

    // 返回在工作进程中实例化的实际读取数据的类
    @Override
    public Class<? extends Task> taskClass() {
        return MySourceTask.class;
    }

    // 核心方法
    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>();
        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<String, String>();
        if (filename != null) {
            config.put(FILE_CONFIG, filename);
        }
        config.put(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }

    // 生命周期方法
    @Override
    public void stop() {
        // Nothing to do since no background monitoring is required.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    // 只有Connector知道如何访问offset,Connector和Task通过initialize方法传递context来访问offset
    @Override
    public void initialize(ConnectorContext context, List<Map<String, String>> taskConfigs) {
        super.initialize(context, taskConfigs);
    }
}
