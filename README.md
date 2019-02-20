# KafkaDemo  [![Travis](https://img.shields.io/badge/KafkaDemo-v1.0.0-green.svg)](https://github.com/huangyueranbbc)  [![Travis](https://img.shields.io/badge/Kafka-API-yellowgreen.svg)](http://kafka.apache.org/0100/javadoc/index.html)  [![Travis](https://img.shields.io/badge/Apache-Kafka-blue.svg)](http://kafka.apache.org/)
1.BaseApi  
2.Streams  常用算子操作
3.Producer  
4.Consumer  
5.Connector  
6.Rebalance  
7.Offset  
8.OffsetAtomic 避免数据丢失或重复消费  
9.多线程消费  
10.优雅的关闭,避免丢失数据  
11.自定义分区器
12.interceptor拦截器


更新中......    

重复消费的原因:  
通常是因为节点故障导致。  
例如当发生了JAVA垃圾回收GC时，所有的线程会在safepoint处进入"stop the world"阻塞停止状态。  
当GC时间过长，worker长时间没有发出心跳，如果超出了配置的间隔期，那么kafka会认为该worker节点挂掉了，  
此时consumer变回执行rebanlance操作。如果过长的gc发生在执行完业务逻辑之后，提交offset之前，  
consumer便会在这个时间段执行rebanlance。此时获取的是提交之前、但是已经消费完毕的offset，便会产生所谓的重复消费。  

The reasons for repeated consumption are:  
It is usually caused by a node failure.  
For example, when the JAVA garbage recycling GC has occurred, all threads enter the "stop the world" blocking stop state at safepoint.  
When the GC is too long, the worker does not make a heartbeat for a long time, and if it goes beyond the configuration interval, then Kafka will think that the worker node is hanging off,  
At this point consumer turns back to execute the rebanlance operation. If the overlong GC occurs after the execution of the business logic, before the offset is submitted,  
Consumer will execute rebanlance at this time. At this point, the offset which has been submitted before, but has already been consumed, will produce so-called repeated consumption.  

