# KafkaDemo  
1.BaseApi  
2.Streams  
3.Producer  
4.Consumer  
5.Connector  
6.Rebalance  
7.Offset  
8.OffsetAtomic 避免数据丢失或重复消费    
  
更新中......    

OffsetAtomic:  
CREATE TABLE `ttt` (  
  `id` bigint(20) NOT NULL AUTO_INCREMENT,  
  `text` varchar(200),  
  PRIMARY KEY (`id`)  
) ENGINE=InnoDB AUTO_INCREMENT=10000000024214 DEFAULT CHARSET=utf8  
  
如果重复消费 会发生主键冲突  