example.conf
```
agent.sources  = source1
agent.channels = channel1
agent.sinks    = sink1

agent.sources.source1.type = org.apache.flume.source.kafka.KafkaSource
agent.sources.source1.channels = channel1
agent.sources.source1.batchSize = 50
agent.sources.source1.batchDurationMillis = 2000
agent.sources.source1.kafka.bootstrap.servers = localhost:9092
agent.sources.source1.kafka.topics = metrics
agent.sources.source1.kafka.consumer.group.id = kafka-json-kudu
agent.sources.source1.channels = channel1

agent.channels.channel1.type                = memory
agent.channels.channel1.capacity            = 10000
agent.channels.channel1.transactionCapacity = 1000

agent.sinks.sink1.type = org.apache.kudu.flume.sink.KuduSink
agent.sinks.sink1.masterAddresses = localhost
agent.sinks.sink1.tableName = metrics
agent.sinks.sink1.channel = channel1
agent.sinks.sink1.batchSize = 50
agent.sinks.sink1.producer = com.yangxy.kudu.flume.sink.JsonKuduOperationsProducer
```


bin/flume-ng agent --conf conf --conf-file conf/example.conf --name a1 -Dflume.root.logger=INFO,console

2018-04-03 14:37:40,995 (conf-file-poller-0) [ERROR - org.apache.flume.node.PollingPropertiesFileConfigurationProvider$FileWatcherRunnable.run(PollingPropertiesFileConfigurationProvider.java:150)] Unhandled error
java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkNotNull(Ljava/lang/Object;Ljava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;
	at org.apache.kudu.flume.sink.KuduSink.configure(KuduSink.java:185)
	at org.apache.flume.conf.Configurables.configure(Configurables.java:41)

升级flume 的guava 到 guava-20.0