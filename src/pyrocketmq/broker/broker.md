# Broker 模块文档

## 概述

消费者和生产者都是先和nameserver建联，获取到topic的路由信息后再与对应broker建联，broker本质也是一组remote服务。

## 模块结构

```
broker/
├── __init__.py          # 模块入口，导出公共API
├── models.py            # 数据结构定义 ✅
├── client.py            # Broker客户端 (待实现)
├── producer.py          # 消息生产者 (待实现)
├── consumer.py          # 消息消费者 (待实现)
├── errors.py            # 异常定义 (待实现)
├── config.py            # 配置管理 (待实现)
├── factory.py           # 工厂函数 (待实现)
├── utils.py             # 工具函数 (待实现)
└── broker.md            # 模块文档
```

## 已完成功能

### 数据结构模型 (models.py) ✅

实现了与RocketMQ Broker通信所需的核心数据结构：

#### 核心数据结构

1. **MessageQueue** - 消息队列信息
   - `topic`: 主题名称
   - `broker_name`: broker名称
   - `queue_id`: 队列ID
   - 支持相等性比较和哈希操作

2. **MessageExt** - 扩展消息信息
   - 包含完整的消息信息（主题、消息体、标签、keys等）
   - 支持系统属性（消息ID、队列ID、偏移量、时间戳等）
   - 支持用户属性管理
   - 事务消息支持

3. **ProducerData** - 生产者信息
   - `group_name`: 生产者组名
   - 用于向Broker注册生产者信息

4. **ConsumerData** - 消费者信息
   - `group_name`: 消费者组名
   - `consume_type`: 消费类型（PUSH/PULL）
   - `message_model`: 消费模式（BROADCASTING/CLUSTERING）
   - `consume_from_where`: 消费起始位置
   - `subscription_data`: 订阅关系

5. **SendMessageResult** - 发送消息结果
   - `msg_id`: 消息ID
   - `queue_id`: 队列ID
   - `queue_offset`: 队列偏移量
   - 支持事务消息信息

6. **PullMessageResult** - 拉取消息结果
   - `messages`: 消息列表
   - `next_begin_offset`: 下次拉取起始偏移量
   - `min_offset`/`max_offset`: 偏移量范围
   - `suggest_which_broker_id`: 建议的broker ID

7. **OffsetResult** - 偏移量查询结果
   - `offset`: 偏移量值
   - `retry_times`: 重试次数

#### 枚举类型

- `ConsumeType`: 消费类型（PUSH/PULL）
- `MessageModel`: 消息模式（BROADCASTING/CLUSTERING）
- `ConsumeFromWhere`: 消费起始位置

#### 设计特性

1. **类型安全**: 使用dataclass和类型注解确保编译时类型检查
2. **Go语言兼容**: 使用ast.literal_eval处理Go语言返回的整数key格式JSON
3. **数据验证**: 包含完整的字段验证和错误处理
4. **序列化支持**: 所有数据结构都支持to_dict/from_dict转换
5. **字节数据处理**: 支持从bytes直接创建实例

#### 使用示例

```python
from pyrocketmq.broker import (
    MessageQueue, MessageExt, ProducerData, ConsumerData,
    SendMessageResult, PullMessageResult, OffsetResult,
    ConsumeType, MessageModel, ConsumeFromWhere
)

# 创建消息队列
mq = MessageQueue(topic="test_topic", broker_name="broker-a", queue_id=0)

# 创建消息
message = MessageExt(
    topic="test_topic",
    body=b"Hello, RocketMQ!",
    tags="test_tag",
    keys=["key1", "key2"]
)
message.set_property("user_key", "user_value")

# 创建生产者信息
producer = ProducerData(group_name="test_producer_group")

# 创建消费者信息
consumer = ConsumerData(
    group_name="test_consumer_group",
    consume_type=ConsumeType.PUSH,
    message_model=MessageModel.CLUSTERING,
    consume_from_where=ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
)

# 从字节数据解析结果
result_bytes = b"{'msgId': 'msg123', 'queueId': 0, 'queueOffset': 100, 'regionId': 'DefaultRegion'}"
send_result = SendMessageResult.from_bytes(result_bytes)
```

## 待实现功能

### 客户端实现 (client.py)
- BrokerClient基础客户端类
- AsyncBrokerClient异步客户端
- 消息发送API
- 消息拉取API
- 偏移量管理API
- 心跳API
- 客户端注册/注销

### 消息生产者 (producer.py)
- DefaultProducer默认生产者实现
- 消息路由选择策略
- 重试机制
- 批量发送支持
- 事务消息支持

### 消息消费者 (consumer.py)
- PushConsumer推式消费者
- PullConsumer拉式消费者
- 消息监听器机制
- 消息确认机制
- 负载均衡策略

### 异常处理 (errors.py)
- BrokerError基础异常
- 各种具体的异常类型
- 分层的异常体系

### 配置管理 (config.py)
- BrokerConfig配置类
- 预定义的配置模板
- 连接池配置

### 工厂函数 (factory.py)
- BrokerClientFactory客户端工厂
- 便捷的创建接口

### 工具函数 (utils.py)
- 消息队列选择算法
- 消息ID生成器
- 压缩/解压缩工具
- 序列化工具

## 测试覆盖

- ✅ 数据结构模型测试：29个测试用例全部通过
- ⏳ 客户端功能测试：待实现
- ⏳ 集成测试：待实现

## 下一步计划

1. 实现异常处理体系 (errors.py)
2. 实现Broker客户端核心功能 (client.py)
3. 实现消息生产者功能 (producer.py)
4. 实现消息消费者功能 (consumer.py)
5. 完善配置和工具类
6. 编写完整的测试套件

Broker中涉及的业务种类繁多，我枚举了一下常规的调用请求(Go实现)有如下：

```go
const (
	ReqSendMessage                   = int16(10)
	ReqPullMessage                   = int16(11)
	ReqQueryMessage                  = int16(12)
	ReqQueryConsumerOffset           = int16(14)
	ReqUpdateConsumerOffset          = int16(15)
	ReqCreateTopic                   = int16(17)
	ReqSearchOffsetByTimestamp       = int16(29)
	ReqGetMaxOffset                  = int16(30)
	ReqGetMinOffset                  = int16(31)
	ReqViewMessageByID               = int16(33)
	ReqHeartBeat                     = int16(34)
	ReqConsumerSendMsgBack           = int16(36)
	ReqENDTransaction                = int16(37)
	ReqGetConsumerListByGroup        = int16(38)
	ReqLockBatchMQ                   = int16(41)
	ReqUnlockBatchMQ                 = int16(42)
	ReqGetRouteInfoByTopic           = int16(105)
	ReqGetBrokerClusterInfo          = int16(106)
	ReqSendBatchMessage              = int16(320)
	ReqCheckTransactionState         = int16(39)
	ReqNotifyConsumerIdsChanged      = int16(40)
	ReqGetAllTopicListFromNameServer = int16(206)
	ReqDeleteTopicInBroker           = int16(215)
	ReqDeleteTopicInNameSrv          = int16(216)
	ReqResetConsumerOffset           = int16(220)
	ReqGetConsumerRunningInfo        = int16(307)
	ReqConsumeMessageDirectly        = int16(309)
	ReqPutMsgNo                      = int16(1001)
	ReqGetMsgNo                      = int16(1011)
)
```

相关数据结构与方法有：

```go
type SendMessageRequestHeader struct {
	ProducerGroup         string
	Topic                 string
	QueueId               int
	SysFlag               int
	BornTimestamp         int64
	Flag                  int32
	Properties            string
	ReconsumeTimes        int
	UnitMode              bool
	MaxReconsumeTimes     int
	Batch                 bool
	DefaultTopic          string
	DefaultTopicQueueNums int
}


func (request *SendMessageRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["producerGroup"] = request.ProducerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	maps["sysFlag"] = fmt.Sprintf("%d", request.SysFlag)
	maps["bornTimestamp"] = strconv.FormatInt(request.BornTimestamp, 10)
	maps["flag"] = fmt.Sprintf("%d", request.Flag)
	maps["reconsumeTimes"] = strconv.Itoa(request.ReconsumeTimes)
	maps["unitMode"] = strconv.FormatBool(request.UnitMode)
	maps["maxReconsumeTimes"] = strconv.Itoa(request.MaxReconsumeTimes)
	maps["defaultTopic"] = "TBW102"
	maps["defaultTopicQueueNums"] = "4"
	maps["batch"] = strconv.FormatBool(request.Batch)
	maps["properties"] = request.Properties

	return maps
}


type EndTransactionRequestHeader struct {
	ProducerGroup        string
	TranStateTableOffset int64
	CommitLogOffset      int64
	CommitOrRollback     int
	FromTransactionCheck bool
	MsgID                string
	TransactionId        string
}

type SendMessageRequestV2Header struct {
	*SendMessageRequestHeader
}

func (request *SendMessageRequestV2Header) Encode() map[string]string {
	maps := make(map[string]string)
	maps["a"] = request.ProducerGroup
	maps["b"] = request.Topic
	maps["c"] = request.DefaultTopic
	maps["d"] = strconv.Itoa(request.DefaultTopicQueueNums)
	maps["e"] = strconv.Itoa(request.QueueId)
	maps["f"] = fmt.Sprintf("%d", request.SysFlag)
	maps["g"] = strconv.FormatInt(request.BornTimestamp, 10)
	maps["h"] = fmt.Sprintf("%d", request.Flag)
	maps["i"] = request.Properties
	maps["j"] = strconv.Itoa(request.ReconsumeTimes)
	maps["k"] = strconv.FormatBool(request.UnitMode)
	maps["l"] = strconv.Itoa(request.MaxReconsumeTimes)
	maps["m"] = strconv.FormatBool(request.Batch)
	return maps
}

func (request *EndTransactionRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["producerGroup"] = request.ProducerGroup
	maps["tranStateTableOffset"] = strconv.FormatInt(request.TranStateTableOffset, 10)
	maps["commitLogOffset"] = strconv.Itoa(int(request.CommitLogOffset))
	maps["commitOrRollback"] = strconv.Itoa(request.CommitOrRollback)
	maps["fromTransactionCheck"] = strconv.FormatBool(request.FromTransactionCheck)
	maps["msgId"] = request.MsgID
	maps["transactionId"] = request.TransactionId
	return maps
}


type CheckTransactionStateRequestHeader struct {
	TranStateTableOffset int64
	CommitLogOffset      int64
	MsgId                string
	TransactionId        string
	OffsetMsgId          string
}

func (request *CheckTransactionStateRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["tranStateTableOffset"] = strconv.FormatInt(request.TranStateTableOffset, 10)
	maps["commitLogOffset"] = strconv.FormatInt(request.CommitLogOffset, 10)
	maps["msgId"] = request.MsgId
	maps["transactionId"] = request.TransactionId
	maps["offsetMsgId"] = request.OffsetMsgId

	return maps
}

func (request *CheckTransactionStateRequestHeader) Decode(properties map[string]string) {
	if len(properties) == 0 {
		return
	}
	if v, existed := properties["tranStateTableOffset"]; existed {
		request.TranStateTableOffset, _ = strconv.ParseInt(v, 10, 0)
	}
	if v, existed := properties["commitLogOffset"]; existed {
		request.CommitLogOffset, _ = strconv.ParseInt(v, 10, 0)
	}
	if v, existed := properties["msgId"]; existed {
		request.MsgId = v
	}
	if v, existed := properties["transactionId"]; existed {
		request.MsgId = v
	}
	if v, existed := properties["offsetMsgId"]; existed {
		request.MsgId = v
	}
}

type ConsumerSendMsgBackRequestHeader struct {
	Group             string
	Offset            int64
	DelayLevel        int
	OriginMsgId       string
	OriginTopic       string
	UnitMode          bool
	MaxReconsumeTimes int32
}

func (request *ConsumerSendMsgBackRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["group"] = request.Group
	maps["offset"] = strconv.FormatInt(request.Offset, 10)
	maps["delayLevel"] = strconv.Itoa(request.DelayLevel)
	maps["originMsgId"] = request.OriginMsgId
	maps["originTopic"] = request.OriginTopic
	maps["unitMode"] = strconv.FormatBool(request.UnitMode)
	maps["maxReconsumeTimes"] = strconv.Itoa(int(request.MaxReconsumeTimes))

	return maps
}

type PullMessageRequestHeader struct {
	ConsumerGroup          string
	Topic                  string
	QueueId                int32
	QueueOffset            int64
	MaxMsgNums             int32
	SysFlag                int32
	CommitOffset           int64
	SuspendTimeoutMillis   time.Duration
	SubExpression          string
	SubVersion             int64
	ExpressionType         string
	SupportCompressionType int
}

func (request *PullMessageRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = fmt.Sprintf("%d", request.QueueId)
	maps["queueOffset"] = fmt.Sprintf("%d", request.QueueOffset)
	maps["maxMsgNums"] = fmt.Sprintf("%d", request.MaxMsgNums)
	maps["sysFlag"] = fmt.Sprintf("%d", request.SysFlag)
	maps["commitOffset"] = fmt.Sprintf("%d", request.CommitOffset)
	maps["suspendTimeoutMillis"] = fmt.Sprintf("%d", request.SuspendTimeoutMillis/time.Millisecond)
	maps["subscription"] = request.SubExpression
	maps["subVersion"] = fmt.Sprintf("%d", request.SubVersion)
	maps["expressionType"] = request.ExpressionType
	maps["supportCompressionType"] = fmt.Sprintf("%d", request.SupportCompressionType)

	return maps
}

type GetConsumerListRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
}

func (request *GetConsumerListRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	return maps
}

type GetMaxOffsetRequestHeader struct {
	Topic   string
	QueueId int
}

func (request *GetMaxOffsetRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	return maps
}

type QueryConsumerOffsetRequestHeader struct {
	ConsumerGroup string
	Topic         string
	QueueId       int
}

func (request *QueryConsumerOffsetRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	return maps
}

type SearchOffsetRequestHeader struct {
	Topic     string
	QueueId   int
	Timestamp int64
}

func (request *SearchOffsetRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	maps["timestamp"] = strconv.FormatInt(request.Timestamp, 10)
	return maps
}

type UpdateConsumerOffsetRequestHeader struct {
	ConsumerGroup string
	Topic         string
	QueueId       int
	CommitOffset  int64
}

func (request *UpdateConsumerOffsetRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	maps["commitOffset"] = strconv.FormatInt(request.CommitOffset, 10)
	return maps
}

type GetRouteInfoRequestHeader struct {
	Topic string
}

func (request *GetRouteInfoRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic
	return maps
}

type GetConsumerRunningInfoHeader struct {
	consumerGroup string
	clientID      string
}

func (request *GetConsumerRunningInfoHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.consumerGroup
	maps["clientId"] = request.clientID
	return maps
}

func (request *GetConsumerRunningInfoHeader) Decode(properties map[string]string) {
	if len(properties) == 0 {
		return
	}
	if v, existed := properties["consumerGroup"]; existed {
		request.consumerGroup = v
	}

	if v, existed := properties["clientId"]; existed {
		request.clientID = v
	}
}

type QueryMessageRequestHeader struct {
	Topic          string
	Key            string
	MaxNum         int
	BeginTimestamp int64
	EndTimestamp   int64
}

func (request *QueryMessageRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic
	maps["key"] = request.Key
	maps["maxNum"] = fmt.Sprintf("%d", request.MaxNum)
	maps["beginTimestamp"] = strconv.FormatInt(request.BeginTimestamp, 10)
	maps["endTimestamp"] = fmt.Sprintf("%d", request.EndTimestamp)

	return maps
}

func (request *QueryMessageRequestHeader) Decode(properties map[string]string) error {
	return nil
}

type ViewMessageRequestHeader struct {
	Offset int64
}

func (request *ViewMessageRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["offset"] = strconv.FormatInt(request.Offset, 10)

	return maps
}

type CreateTopicRequestHeader struct {
	Topic           string
	DefaultTopic    string
	ReadQueueNums   int
	WriteQueueNums  int
	Perm            int
	TopicFilterType string
	TopicSysFlag    int
	Order           bool
}

func (request *CreateTopicRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic
	maps["defaultTopic"] = request.DefaultTopic
	maps["readQueueNums"] = fmt.Sprintf("%d", request.ReadQueueNums)
	maps["writeQueueNums"] = fmt.Sprintf("%d", request.WriteQueueNums)
	maps["perm"] = fmt.Sprintf("%d", request.Perm)
	maps["topicFilterType"] = request.TopicFilterType
	maps["topicSysFlag"] = fmt.Sprintf("%d", request.TopicSysFlag)
	maps["order"] = strconv.FormatBool(request.Order)

	return maps
}

type TopicListRequestHeader struct {
	Topic string
}

func (request *TopicListRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic

	return maps
}

type DeleteTopicRequestHeader struct {
	Topic string
}

func (request *DeleteTopicRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic

	return maps
}

type ResetOffsetHeader struct {
	topic     string
	group     string
	timestamp int64
	isForce   bool
}

func (request *ResetOffsetHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.topic
	maps["group"] = request.group
	maps["timestamp"] = strconv.FormatInt(request.timestamp, 10)
	return maps
}

func (request *ResetOffsetHeader) Decode(properties map[string]string) {
	if len(properties) == 0 {
		return
	}

	if v, existed := properties["topic"]; existed {
		request.topic = v
	}

	if v, existed := properties["group"]; existed {
		request.group = v
	}

	if v, existed := properties["timestamp"]; existed {
		request.timestamp, _ = strconv.ParseInt(v, 10, 0)
	}
}

type ConsumeMessageDirectlyHeader struct {
	consumerGroup string
	clientID      string
	msgId         string
	brokerName    string
}

func (request *ConsumeMessageDirectlyHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.consumerGroup
	maps["clientId"] = request.clientID
	maps["msgId"] = request.msgId
	maps["brokerName"] = request.brokerName
	return maps
}

func (request *ConsumeMessageDirectlyHeader) Decode(properties map[string]string) {
	if len(properties) == 0 {
		return
	}

	if v, existed := properties["consumerGroup"]; existed {
		request.consumerGroup = v
	}

	if v, existed := properties["clientId"]; existed {
		request.clientID = v
	}

	if v, existed := properties["msgId"]; existed {
		request.msgId = v
	}

	if v, existed := properties["brokerName"]; existed {
		request.brokerName = v
	}
}

type SaveOrGetMsgNoHeader struct {
	msgNo string
}


func (request *SaveOrGetMsgNoHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["msgNo"] = request.msgNo
	return maps
}
```
