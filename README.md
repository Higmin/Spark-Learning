# SparkMovie
Spark观众电影分析，用户实时点击统计（Kafka + SparkStreaming + Redis）  
spark 版本-2.4.4  
spark-streaming_2.12 版本-2.4.4  
spark-streaming-kafka-0-10_2.12 版本-2.4.4  

### 数据来源：https://grouplens.org/datasets/movielens/  

## Spark
### Spark 任务一：统计看过 “Sixteen Candles” 的用户、性别和观看次数  
### Spark 任务二：统计年龄段在20-30的年轻人，最喜欢看哪10部电影  
### Spark 任务三：最受欢迎的前三部电影(平均评分最高的三部电影)  

## Kafka + SparkStreaming + Redis
### Kafka + SparkStreaming + Redis 模拟 Kafka 生产者 实时写入用户行为的事件数据，数据是JSON格式  
### Kafka + SparkStreaming + Redis scala实现Redis 客户端
### Kafka + SparkStreaming + Redis 实现实时统计每个用户的点击次数，它是按照用户分组进行累加次数
### Kafka + SparkStreaming + Redis 每5秒 统计 过去10秒 每种终端 收到的点击量
注意：
  * 1. 使用 SparkStreaming窗口 计算需要设置检查点 checkpoint
  * 2. 窗口滑动长度和窗口长度一定要是SparkStreaming微批处理时间的整数倍,不然会报错
