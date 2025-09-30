nameserver就是我们获取broker信息的介质，所以nameserver的api相对来说不会有很多

翻了一下Go的代码，nameserver对客户端主要暴露了2个api：

- query_topic_route_info 获取指定topic的路由信息；
- get_broker_cluster_info 获取该nameserver上所有注册的broker信息；

go语言实现中相关数据结构如下：

```go

func getBrokerClusterInfo() (*BrokerClusterInfo, error)
type BrokerClusterInfo struct {
	BrokerAddrTable  map[string]*BrokerData `json:"brokerAddrTable"`
	ClusterAddrTable map[string][]string    `json:"clusterAddrTable"`
}

type BrokerData struct {
	Cluster             string           `json:"cluster"`
	BrokerName          string           `json:"brokerName"`
	BrokerAddresses     map[int64]string `json:"brokerAddrs"`
	brokerAddressesLock sync.RWMutex
}

type TopicRouteData struct {
	OrderTopicConf string
	QueueDataList  []*QueueData  `json:"queueDatas"`
	BrokerDataList []*BrokerData `json:"brokerDatas"`
}



func queryTopicRouteInfoFromServer(topic string) (*TopicRouteData, error)
type QueueData struct {
	BrokerName      string `json:"brokerName"`
	ReadQueueNums   int    `json:"readQueueNums"`
	WriteQueueNums  int    `json:"writeQueueNums"`
	Perm            int    `json:"perm"`
	TopicSynFlag    int    `json:"topicSynFlag"`
	CompressionType string `json:"compressionType"` // gzip|zstd
}

type BrokerData struct {
	Cluster             string           `json:"cluster"`
	BrokerName          string           `json:"brokerName"`
	BrokerAddresses     map[int64]string `json:"brokerAddrs"`
}
```
