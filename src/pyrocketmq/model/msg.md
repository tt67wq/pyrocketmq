Rocketmq消息在tcp中流通的格式协议如下：
```
| length | header-length | header-data | body-data |
```

其中后两部分是通讯的实际数据。前两段都是4byte的整数，分别表示两段实际数据的长度。
- length: 帧长度，4byte整数；
- header-length:  header的长度。4byte整数；
- header: 协议头，后面解释
- body: 消息体，例如生产者发的消息内容就放在这里。

header包含了一个消息的元数据，包括以下信息：
- code: 请求的枚举，整型；
- language: 客户端语言枚举，整型；
- opaque：消息自增id，整型；
- flag：通信的类型枚举，整型；
- remark：文本，通常用于存储错误信息，字符串类型；
- ext_fields: 元数据，二进制类型；

参考go的数据结构：
```go
type RemotingCommand struct {
	Code      int16             `json:"code"`
	Language  LanguageCode      `json:"language"`
	Version   int16             `json:"version"`
	Opaque    int32             `json:"opaque"`
	Flag      int32             `json:"flag"`
	Remark    string            `json:"remark"`
	ExtFields map[string]string `json:"extFields"`
	Body      []byte            `json:"-"`
}
```
