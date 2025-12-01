# 核心架构

## 分层架构设计
项目采用清晰的分层架构，从底层协议实现到高级客户端功能，每一层都有明确的职责分工：

1. **协议模型层** (`model/`): 定义RocketMQ TCP协议的数据结构和序列化机制
2. **网络传输层** (`transport/`): 基于状态机的TCP连接管理，提供可靠的字节流传输
3. **远程通信层** (`remote/`): 异步/同步RPC通信和连接池管理，提供高级通信抽象
4. **注册发现层** (`nameserver/`): NameServer客户端，提供路由查询和集群管理
5. **Broker通信层** (`broker/`): Broker客户端封装，提供消息收发等核心功能
6. **高级应用层**: 
   - **producer/**: 消息生产者实现，包含路由、事务等高级特性
   - **consumer/**: 消息消费者实现，包含订阅管理、偏移量存储、消息监听等核心功能
7. **工具支持层** (`utils/`): 读写锁、线程安全工具等
8. **日志系统层** (`logging/`): 统一的日志记录和管理系统

## 模块依赖关系

### 依赖层次图
```
┌─────────────────────────────────────────────────────────────┐
│                    应用层 (Application)                        │
├─────────────────────────────────────────────────────────────┤
│                Producer层 & Consumer层 (高级功能)              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   Producer      │  │   Consumer      │  │TransactionProd │ │
│  │                 │  │                 │  │                 │ │
│  │ AsyncProducer   │  │ ConcurrentCons  │  │ AsyncTransaction│ │
│  │                 │  │ OrderlyConsumer │  │     Producer    │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│              客户端层 (NameServer & Broker)                   │
│  ┌─────────────────┐              ┌─────────────────┐         │
│  │  NameServer     │              │     Broker      │         │
│  │     Client      │              │     Client      │         │
│  └─────────────────┘              └─────────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                 远程通信层 (Remote)                           │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │     Remote      │  │   AsyncRemote   │  │ ConnectionPool  │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│               传输层 (Transport)                              │
│  ┌─────────────────┐  ┌─────────────────┐                     │
│  │ConnectionState  │  │AsyncConnection  │                     │
│  │    Machine      │  │   StateMachine  │                     │
│  └─────────────────┘  └─────────────────┘                     │
├─────────────────────────────────────────────────────────────┤
│                  协议模型层 (Model)                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │ RemotingCommand │  │     Message     │  │ RequestFactory  │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                   工具支持层 (Utils)                           │
│  ┌─────────────────┐                                         │
│  │   SyncRWLock    │                                         │
│  └─────────────────┘                                         │
├─────────────────────────────────────────────────────────────┤
│                   日志系统 (Logging)                           │
│              (贯穿所有层，提供统一日志服务)                      │
└─────────────────────────────────────────────────────────────┘
```

### 模块间依赖关系

**Producer模块依赖**:
```
Producer → {MessageRouter, TopicBrokerMapping, Config}
    ↓
MessageRouter → {QueueSelector, TopicBrokerMapping}
    ↓
TopicBrokerMapping → {Model (数据结构)}
    ↓
Producer → {BrokerManager, NameServerManager}
    ↓
BrokerManager/NameServerManager → {Remote, AsyncRemote}
    ↓
Remote/AsyncRemote → {ConnectionPool, Transport}
    ↓
Transport → {ConnectionStateMachine}
    ↓
所有模块 → {Logging (日志记录)}
```

**数据流向**:
1. **应用请求**: Producer.send(message)
2. **路由决策**: MessageRouter.route_message()
3. **连接获取**: BrokerManager.connection()
4. **网络传输**: Remote.send_request()
5. **协议序列化**: Model.Serializer.serialize()
6. **TCP传输**: Transport.send_data()
7. **日志记录**: 贯穿所有步骤