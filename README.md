# SparkMovie
Spark观众电影分析，用户实时点击统计（Kafka + SparkStreaming + Redis）  
spark 版本-2.4.4  
spark-streaming_2.12 版本-2.4.4  
spark-streaming-kafka-0-10_2.12 版本-2.4.4  

### 数据来源：https://grouplens.org/datasets/movielens/  
### 本地测试方法：  
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

## 2.Kafka + SparkStreaming + Redis
Kafka + SparkStreaming + Redis 模拟 Kafka 生产者 实时写入用户行为的事件数据，数据是JSON格式  
Kafka + SparkStreaming + Redis scala实现Redis 客户端  
Kafka + SparkStreaming + Redis 实现实时统计每个用户的点击次数，它是按照用户分组进行累加次数  
Kafka + SparkStreaming + Redis 每5秒 统计 过去10秒 每种终端 收到的点击量  
注意：
  * 1. 使用 SparkStreaming窗口 计算需要设置检查点 checkpoint
  * 2. 窗口滑动长度和窗口长度一定要是SparkStreaming微批处理时间的整数倍,不然会报错
  
## 3.Kafka + SparkStreaming + mysql
Kafka + SparkStreaming + mysql 模拟 Kafka 生产者 实时写入用户行为的事件数据，数据是JSON格式  
Kafka + SparkStreaming + mysql java实现 mysql连接池（本例中使用阿里开源的 druid 连接池 ）
Kafka + SparkStreaming + mysql 实现实时统计每个用户的点击次数，它是按照用户分组进行累加次数  
Kafka + SparkStreaming + mysql 每5秒 统计 过去10秒 每种终端 收到的点击量 
