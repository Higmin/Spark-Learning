## 欢迎大家来到 Higmin/SparkObject
# 1. Spark离线批处理 => 受众电影分析
###  (数据来源：https://grouplens.org/datasets/movielens )
# 2. SparkSQL内容分析 => Weblog 网站日志分析(数据源：log文件)、工业级传感器数据分析(数据源：Hbase)
# 3. spark-streaming => 用户实时点击统计(数据源：kafka)
Spark受众电影分析，用户实时点击统计（Kafka + SparkStreaming + Redis）  
spark 版本-2.4.4  
spark-streaming_2.12 版本-2.4.4  
spark-streaming-kafka-0-10_2.12 版本-2.4.4  
spark-sql_2.11 版本-2.4.4  

### 本地测试方法 ：  
1. 本地安装Spark 2.4.4 ,redis（往redis存需要安装redis） ， mysql（往 mysql 存需要安装 mysql ）  
  => MySQL相关说明：用户名 root ,密码 root 。创建数据库test,
  => 创建数据表 streaming [uid（varchar 255）; clickCount （varchar 255）] 
  => 创建数据表treaming_ostype[os_type（varchar 255）; clickCount （varchar 255）]
2. 下载示例代码  
3. Kafka + SparkStreaming + Redis 和 Kafka + SparkStreaming + mysql 运行流程相同  
4. 启动 kafka 消息模拟生产者
5. 启动 sparkStreaming 实时计算任务。
结果：可在 redis 或者 mysql 中查看

## 1.Spark
Spark 任务一：统计看过 “Sixteen Candles” 的用户、性别和观看次数  
Spark 任务二：统计年龄段在20-30的年轻人，最喜欢看哪10部电影  
Spark 任务三：最受欢迎的前三部电影(平均评分最高的三部电影)  
##### 代码详情：https://github.com/Higmin/SparkObject/tree/master/src/main/scala/org/spark/movie

## 2.Spark SQL 内容分析
SparkSQL是spark用来处理结构化的一个模块，它提供一个抽象的数据集DataFrame,并且是作为分布式SQL查询引擎的应用。  
SparkSQL实现了Hive兼容，执行计划生成和优化都由Catalyst负责。借助Scala的模式匹配等函数式语言特性，利用Catalyst开发执行计划优化策略比Hive要简洁得多。  
关于Dataframe和Dataset：Dataframe/Dataset也是分布式数据集，但与RDD不同的是其带有schema信息，类似一张表。Dataset是在spark1.6引入的，目的是提供像RDD一样的强类型、使用强大的lambda函数，同时使用spark sql的优化执行引擎。到spark2.0以后，DataFrame变成类型为Row的Dataset，即为：
``
type DataFrame = Dataset[Row]
``  
  #### 2.1 这里是一个SparkSQL读取Weblog 日志文件的数据分析实例：  
##### 详情请参考：https://github.com/Higmin/SparkObject/tree/master/src/main/scala/org/sparkSQL

  #### 2.2 另外还有spark 读取 Hbase 中的数据，并转换为 DataFrame ,利用SparkSQL 进行数据分析：
##### 详情请参考：https://github.com/Higmin/SparkObject/tree/master/src/main/scala/org/sparkSQL/SensorLog  
Hbsse 表数据结构如下：（以下列举一条数据）  

rowKey  |   columnFamily   |   column              |       	value     
--------|------------------|-----------------------|-----------------
2314035476751 | info | AndroidBoard::availableStorage	|	12379  
2314035476751 | info |  AndroidBoard::cpu_usage		    |	4.32  
2314035476751 | info | AndroidBoard::current		    | 	1.3  
2314035476751 | info | AndroidBoard::storage		    | 	12661  
2314035476751 | info | AndroidBoard::voltage	        | 	11.8  
2314035476751 | info | AxialFanSpeed			        | 	56.666666666666664  
2314035476751 | info | BackLight::state			        | 	true  
2314035476751 | info | BlEnable::state				    | 	true  	
2314035476751 | info | CrossFlowFanSpeed::value		    | 	40  
2314035476751 | info | Decibel::value				    | 	40  	
2314035476751 | info | DoorState::state			        | 	false  
2314035476751 | info | GPS::latitude			        | 	39.950565  	
2314035476751 | info | GPS::longitude			        | 	116.500711  	
2314035476751 | info | Humidity::value			        | 	40  
2314035476751 | info | Level::value				        | 	80  
2314035476751 | info | PowerState::state		        | 	false  
2314035476751 | info | PowerState::value		        | 	0.8  
2314035476751 | info | Temperature				        | 	29.0  	
2314035476751 | info | Time						        | 	1576745304132  	
2314035476751 | info | TotalPower::value		        | 	900  

## 3.Kafka + SparkStreaming + Redis
Kafka + SparkStreaming + Redis 模拟 Kafka 生产者 实时写入用户行为的事件数据，数据是JSON格式  
Kafka + SparkStreaming + Redis scala实现Redis 客户端  
Kafka + SparkStreaming + Redis 实现实时统计每个用户的点击次数，它是按照用户分组进行累加次数  
Kafka + SparkStreaming + Redis 每5秒 统计 过去10秒 每种终端 收到的点击量  
注意：
  * 1. 使用 SparkStreaming窗口 计算需要设置检查点 checkpoint
  * 2. 窗口滑动长度和窗口长度一定要是SparkStreaming微批处理时间的整数倍,不然会报错
##### 代码详情：https://github.com/Higmin/SparkObject/tree/master/src/main/scala/org/sparkStreaming/kafka_sparkStreaming_redis
  
## 4.Kafka + SparkStreaming + mysql
Kafka + SparkStreaming + mysql 模拟 Kafka 生产者 实时写入用户行为的事件数据，数据是JSON格式  
Kafka + SparkStreaming + mysql java实现 mysql连接池（本例中使用阿里开源的 druid 连接池 ）  
Kafka + SparkStreaming + mysql 实现实时统计每个用户的点击次数，它是按照用户分组进行累加次数  
Kafka + SparkStreaming + mysql 每5秒 统计 过去10秒 每种终端 收到的点击量  
注意：
  * 1. 使用 SparkStreaming窗口 计算需要设置检查点 checkpoint
  * 2. 窗口滑动长度和窗口长度一定要是SparkStreaming微批处理时间的整数倍,不然会报错  
##### 代码详情：https://github.com/Higmin/SparkObject/tree/master/src/main/scala/org/sparkStreaming/kafka_sparkStreaming_mysql

## 5.Spark Streaming 中实现 Exactly-Once 语义
Exactly-once 语义是实时计算的难点之一。  
要做到每一条记录只会被处理一次，即使服务器或网络发生故障时也能保证没有遗漏，这不仅需要实时计算框架本身的支持，还对上游的消息系统、下游的数据存储有所要求。此外，我们在编写计算流程时也需要遵循一定规范，才能真正实现 Exactly-once。  

实时计算有三种语义，分别是 At-most-once、At-least-once、以及 Exactly-once。一个典型的 Spark Streaming 应用程序会包含三个处理阶段：接收数据、处理汇总、输出结果。每个阶段都需要做不同的处理才能实现相应的语义。  
对于 接收数据，主要取决于上游数据源的特性。例如，从 HDFS 这类支持容错的文件系统中读取文件，能够直接支持 Exactly-once 语义。如果上游消息系统支持 ACK（如RabbitMQ），我们就可以结合 Spark 的 Write Ahead Log 特性来实现 At-least-once 语义。对于非可靠的数据接收器（如 socketTextStream），当 Worker 或 Driver 节点发生故障时就会产生数据丢失，提供的语义也是未知的。而 Kafka 消息系统是基于偏移量（Offset）的，它的 Direct API 可以提供 Exactly-once 语义。  
在使用 Spark RDD 对数据进行 转换或汇总 时，我们可以天然获得 Exactly-once 语义，因为 RDD 本身就是一种具备容错性、不变性、以及计算确定性的数据结构。只要数据来源是可用的，且处理过程中没有副作用（Side effect），我们就能一直得到相同的计算结果。  

实时计算中的 Exactly-once 是比较强的一种语义，因而会给你的应用程序引入额外的开销。此外，它尚不能很好地支持窗口型操作。因此，是否要在代码中使用这一语义就需要开发者自行判断了。很多情况下，数据丢失或重复处理并不那么重要。不过，了解 Exactly-once 的开发流程还是有必要的，对学习 Spark Streaming 也会有所助益。  
##### 代码详情：https://github.com/Higmin/SparkObject/tree/master/src/main/scala/org/sparkStreaming/sparkStreamingExactltyOnce

## 6.Kafka + SparkStreaming手动管理 offset
为了应对可能出现的引起Streaming程序崩溃的异常情况，我们一般都需要手动管理好Kafka的offset，而不是让它自动提交，即需要将enable.auto.commit设为false。只有管理好offset，才能使整个流式系统最大限度地接近exactly once语义。  
Offsets可以通过多种方式来管理，但是一般来说遵循下面的步骤:

1. 在 Direct DStream初始化的时候，需要指定一个包含每个topic的每个分区的offset用于让Direct DStream从指定位置读取数据。
 （offsets就是步骤4中所保存的offsets位置）
2. 读取并处理消息

3. 处理完之后存储结果数据

用虚线圈存储和提交offset只是简单强调用户可能会执行一系列操作来满足他们更加严格的语义要求。这包括幂等操作和通过原子操作的方式存储offset。

4.最后，将offsets保存在外部持久化数据库如 HBase, Kafka, HDFS, and ZooKeeper中    
##### 参考博客：https://blog.csdn.net/rlnLo2pNEfx9c/article/details/79988218  
  #### 6.1 存储在kafka本身 (注意： commitAsync()是Spark Streaming集成kafka-0-10版本中的，在Spark文档提醒到它仍然是个实验性质的API并且存在修改的可能性。)
```markdown
stream.foreachRDD { rdd =>
  val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

  // some time later, after outputs have completed
  stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
}
```
  #### 6.2 存储在zookeeper等外部存储  
##### 详情请参考：https://github.com/Higmin/SparkObject/tree/master/src/main/scala/org/sparkStreaming/kafka_sparkStreaming_offsetToZK

  #### 6.3 为什么不用SparkStreaming 的 checkpoint?  
Spark Streaming的checkpoint机制无疑是用起来最简单的，checkpoint数据存储在HDFS中，如果Streaming应用挂掉，可以快速恢复。  
但是，如果Streaming程序的代码改变了，重新打包执行就会出现反序列化异常的问题。这是因为checkpoint首次持久化时会将整个jar包序列化，以便重启时恢复。重新打包之后，新旧代码逻辑不同，就会报错或者仍然执行旧版代码。  
要解决这个问题，只能将HDFS上的checkpoint文件删掉，但这样也会同时删掉Kafka的offset信息，就毫无意义了。  

