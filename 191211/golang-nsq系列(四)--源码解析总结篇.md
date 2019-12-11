## 1. 前言：为什么要使用 MQ 消息队列

随着互联网技术在各行各业的应用高速普及与发展，各层应用之间调用关系越来越复杂，架构、开发、运维成本越来越高，高内聚、低耦合、可扩展、高可用已成为了行业需求。

一提到消息队列 `MQ(Message Queue)`，我们会想到很多应用场景，比如消息通知、用户积分增减、抽奖中奖等，可以看出来 `MQ` 的作用有：
流程异步化、代码解耦合、流量削峰、高可用、高吞吐量、广播分发，达到数据的最终一致性，满足具体的业务场景需求。

本文将从 `MQ` 比较、`NSQ` 介绍、源代码逻辑、亮点小结等方面进行解析，以期对 `NSQ` 有较为深入的理解。

## 2. 主流 MQ 比较

目前主流的 `MQ` 有 `Kafka`, `RabbitMQ`, `NSQ`, `RocketMQ`, `ActiveMQ`，它们的对比如下：

![mq_compare](https://user-gold-cdn.xitu.io/2019/12/11/16ef592c81d49f1b?w=1851&h=964&f=png&s=131247)

## 3. NSQ 初识

`NSQ` 最初是由 `bitly` 公司开源出来的一款简单易用的分布式消息中间件，它可用于大规模系统中的实时消息服务，并且每天能够处理数亿级别的消息。

![nsq](https://user-gold-cdn.xitu.io/2019/12/11/16ef5932cc2f28e7?w=256&h=256&f=png&s=8317)

### 3.1 NSQ 特性

**分布式：** 它提供了分布式的、去中心化且没有单点故障的拓扑结构，稳定的消息传输发布保障，能够具有高容错和高可用特性。

**易于扩展：** 它支持水平扩展，没有中心化的消息代理（ `Broker` ），内置的发现服务让集群中增加节点非常容易。

**运维方便：** 它非常容易配置和部署，灵活性高。

**高度集成：** 现在已经有官方的 `Golang`、`Python` 和 `JavaScript` 客户端，社区也有了其他各个语言的客户端库方便接入，自定义客户端也非常容易。

### 3.2 NSQ 组件

![nsq](https://user-gold-cdn.xitu.io/2019/12/11/16ef594397ff1ac8?w=420&h=281&f=gif&s=31325)

`Topic`：一个 `topic` 就是程序发布消息的一个逻辑键，当程序第一次发布消息时就会创建 `topic`。

`Channels`： `channel` 与消费者相关，是消费者之间的负载均衡， `channel` 在某种意义上来说是一个“队列”。每当一个发布者发送一条消息到一个 `topic`，消息会被复制到所有消费者连接的 channel 上，消费者通过这个特殊的 channel 读取消息，实际上，在消费者第一次订阅时就会创建 `channel`。 `Channel` 会将消息进行排列，如果没有消费者读取消息，消息首先会在内存中排队，当量太大时就会被保存到磁盘中。

`Messages`：消息构成了我们数据流的中坚力量，消费者可以选择结束消息，表明它们正在被正常处理，或者重新将他们排队待到后面再进行处理。每个消息包含传递尝试的次数，当消息传递超过一定的阀值次数时，我们应该放弃这些消息，或者作为额外消息进行处理。

`nsqd`： `nsqd` 是一个守护进程，负责接收（生产者 `producer` ）、排队（最小堆 `min heap` 实现）、投递（消费者 `consumer` ）消息给客户端。它可以独立运行，不过通常它是由 `nsqlookupd` 实例所在集群配置的（它在这能声明 `topics` 和 `channels`，以便大家能找到）。

`nsqlookupd`： `nsqlookupd` 是守护进程负责管理拓扑信息。客户端通过查询 `nsqlookupd` 来发现指定话题（ `topic` ）的生产者，并且 `nsqd` 节点广播话题（`topic`）和通道（ `channel` ）信息。有两个接口： `TCP` 接口， `nsqd` 用它来广播。 `HTTP` 接口，客户端用它来发现和管理。

![topology](https://user-gold-cdn.xitu.io/2019/12/11/16ef5943982c6313?w=584&h=480&f=png&s=143959)

`nsqadmin`： `nsqadmin` 是一套 `WEB UI`，用来汇集集群的实时统计，并执行不同的管理任务。 常用工具类：

`nsq_to _file`：消费指定的话题（`topic`）/通道（`channel`），并写到文件中，有选择的滚动和/或压缩文件。

`nsq_to _http`：消费指定的话题（`topic`）/通道（`channel`）和执行 `HTTP requests (GET/POST)` 到指定的端点。

`nsq_to _nsq`：消费者指定的话题/通道和重发布消息到目的地 `nsqd` 通过 `TCP`。

## 4. nsqd 源码解析

### 4.1 nsqd 执行入口

在 `nsq/apps/nsqd/main.go` 可以找到执行入口文件，如下：

![nsqd_main](https://user-gold-cdn.xitu.io/2019/12/11/16ef59690708d1da?w=375&h=429&f=png&s=28213)

### 4.2 nsqd 执行主逻辑源码

a. 通过第三方 `svc` 包进行优雅的后台进程管理，`svc.Run() -> svc.Init() -> svc.Start()`，启动 `nsqd` 实例；

```go
func main() {
  prg := &program{}
  if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
    logFatal("%s", err)
  }
}

func (p *program) Init(env svc.Environment) error {
  if env.IsWindowsService() {
    dir := filepath.Dir(os.Args[0])
    return os.Chdir(dir)
  }
  return nil
}

func (p *program) Start() error {
  opts := nsqd.NewOptions()

  flagSet := nsqdFlagSet(opts)
  flagSet.Parse(os.Args[1:])
  ...
}
```

b. 初始化配置项（ `opts, cfg` ），加载历史数据（ `nsqd.LoadMetadata` ）、持久化最新数据（ `nsqd.PersistMetadata` ），然后开启协程，进入 `nsqd.Main()` 主函数；

```go
options.Resolve(opts, flagSet, cfg)
  nsqd, err := nsqd.New(opts)
  if err != nil {
    logFatal("failed to instantiate nsqd - %s", err)
  }
  p.nsqd = nsqd

  err = p.nsqd.LoadMetadata()
  if err != nil {
    logFatal("failed to load metadata - %s", err)
  }
  err = p.nsqd.PersistMetadata()
  if err != nil {
    logFatal("failed to persist metadata - %s", err)
  }

  go func() {
    err := p.nsqd.Main()
    if err != nil {
      p.Stop()
      os.Exit(1)
    }
  }()
```

c. 初始化 `tcpServer, httpServer, httpsServer`，然后循环监控队列信息（ `n.queueScanLoop` ）、节点信息管理（ `n.lookupLoop` ）、统计信息（ `n.statsdLoop` ）输出；

```go
tcpServer := &tcpServer{ctx: ctx}
  n.waitGroup.Wrap(func() {
    exitFunc(protocol.TCPServer(n.tcpListener, tcpServer, n.logf))
  })
  httpServer := newHTTPServer(ctx, false, n.getOpts().TLSRequired == TLSRequired)
  n.waitGroup.Wrap(func() {
    exitFunc(http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf))
  })
  if n.tlsConfig != nil && n.getOpts().HTTPSAddress != "" {
    httpsServer := newHTTPServer(ctx, true, true)
    n.waitGroup.Wrap(func() {
      exitFunc(http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf))
    })
  }

  n.waitGroup.Wrap(n.queueScanLoop)
  n.waitGroup.Wrap(n.lookupLoop)
  if n.getOpts().StatsdAddress != "" {
    n.waitGroup.Wrap(n.statsdLoop)
  }
```

d. 分别处理 `tcp/http` 请求，开启 `handler` 协程进行并发处理，其中 `newHTTPServer` 注册路由采用了 `Decorate` 装饰器模式（后面会进一步解析）；

**`http-Decorate` 路由分发**

```go
router := httprouter.New()
  router.HandleMethodNotAllowed = true
  router.PanicHandler = http_api.LogPanicHandler(ctx.nsqd.logf)
  router.NotFound = http_api.LogNotFoundHandler(ctx.nsqd.logf)
  router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqd.logf)
  s := &httpServer{
    ctx:         ctx,
    tlsEnabled:  tlsEnabled,
    tlsRequired: tlsRequired,
    router:      router,
  }

  router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))
  router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.V1))

  // v1 negotiate
  router.Handle("POST", "/pub", http_api.Decorate(s.doPUB, http_api.V1))
  router.Handle("POST", "/mpub", http_api.Decorate(s.doMPUB, http_api.V1))
  router.Handle("GET", "/stats", http_api.Decorate(s.doStats, log, http_api.V1))

  // only v1
  router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
  router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
```

**`tcp-handler` 处理**

```go
for {
    clientConn, err := listener.Accept()
    if err != nil {
      if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
        logf(lg.WARN, "temporary Accept() failure - %s", err)
        runtime.Gosched()
        continue
      }
      // theres no direct way to detect this error because it is not exposed
      if !strings.Contains(err.Error(), "use of closed network connection") {
        return fmt.Errorf("listener.Accept() error - %s", err)
      }
      break
    }
    go handler.Handle(clientConn)
  }
```

e. `TCP` 解析 `V2` 协议，走内部协议封装的 `prot.IOLoop(conn)` 进行处理；

```go
var prot protocol.Protocol
  switch protocolMagic {
  case "  V2":
    prot = &protocolV2{ctx: p.ctx}
  default:
    protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
    clientConn.Close()
    p.ctx.nsqd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
      clientConn.RemoteAddr(), protocolMagic)
    return
  }

  err = prot.IOLoop(clientConn)
  if err != nil {
    p.ctx.nsqd.logf(LOG_ERROR, "client(%s) - %s", clientConn.RemoteAddr(), err)
    return
  }
```

f. 通过内部协议进行 `p.Exec` （执行命令）、 `p.Send` （发送结果），保证每个 `nsqd` 节点都能正确的进行消息生成与消费，一旦上述过程有 `error` 都会被捕获处理，确保分布式投递的可靠性；

```go
params := bytes.Split(line, separatorBytes)

    p.ctx.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %s", client, params)

    var response []byte
    response, err = p.Exec(client, params)
    if err != nil {
      ctx := ""
      if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
        ctx = " - " + parentErr.Error()
      }
      p.ctx.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

      sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
      if sendErr != nil {
        p.ctx.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
        break
      }

      // errors of type FatalClientErr should forceably close the connection
      if _, ok := err.(*protocol.FatalClientErr); ok {
        break
      }
      continue
    }

    if response != nil {
      err = p.Send(client, frameTypeResponse, response)
      if err != nil {
        err = fmt.Errorf("failed to send response - %s", err)
        break
      }
    }
```

## 4.3 nsqd 流程图小结

上述流程小结如下：

![nsqd](https://user-gold-cdn.xitu.io/2019/12/11/16ef59501c5735c7?w=1026&h=1593&f=png&s=114890)

## 5. nsqlookupd 源码解析

`nsqlookupd` 代码执行逻辑与 `nsqd` 大体相似，小结流程图如下：

![nsqlookupd](https://user-gold-cdn.xitu.io/2019/12/11/16ef5955530c5950?w=1047&h=1247&f=png&s=88595)

## 6. 源码亮点

### 6.1 使用装饰器

从路由 `router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))`，可以看出 `httpServer` 通过 `http_api.Decorate` 装饰器实现对各 `http` 路由进行 `handler` 装饰，如加 `log` 日志、`V1` 协议版本号的统一格式输出等；

```go
func Decorate(f APIHandler, ds ...Decorator) httprouter.Handle {
  decorated := f
  for _, decorate := range ds {
    decorated = decorate(decorated)
  }
  return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
    decorated(w, req, ps)
  }
}
```

### 6.2 锁与原子操作 RWMutex/atomic.Value

从下面的代码中可以看到，当需要获取一个 `topic` 的时候，先用读锁去读(此时如果有写锁将被阻塞)，若存在则直接返回，若不存在则使用写锁新建一个；另外，使用 `atomic.Value` 进行结构体某些字段的并发存取值，保证原子性。

```go
func (n *NSQD) GetTopic(topicName string) *Topic {
  // most likely, we already have this topic, so try read lock first.
  n.RLock()
  t, ok := n.topicMap[topicName]
  n.RUnlock()
  if ok {
    return t
  }

  n.Lock()

  t, ok = n.topicMap[topicName]
  if ok {
    n.Unlock()
    return t
  }
  deleteCallback := func(t *Topic) {
    n.DeleteExistingTopic(t.name)
  }
  t = NewTopic(topicName, &context{n}, deleteCallback)
  n.topicMap[topicName] = t

  n.Unlock()
}
```

### 6.3 消息多路分发 & 负载均衡

`Topic` 和 `Channel` 都没有预先配置。`Topic` 由第一次发布消息到命名的 `Topic` 或第一次通过订阅一个命名 `Topic` 来创建。`Channel` 被第一次订阅到指定的 `Channel` 创建。`Topic` 和 `Channel` 的所有缓冲的数据相互独立，防止缓慢消费者造成对其他 `Channel` 的积压（同样适用于 `Topic` 级别）。

**多路分发** - `producer` 会同时连上 `nsq` 集群中所有 `nsqd` 节点，当然这些节点的地址是在初始化时，通过外界传递进去；当发布消息时，`producer` 会随机选择一个 `nsqd` 节点发布某个 `Topic` 的消息；`consumer` 在订阅 `subscribe` 某个`Topic/Channel`时，会首先连上 `nsqlookupd` 获取最新可用的 `nsqd` 节点，然后通过 `TCP` 长连接方式连上所有发布了指定 `Topic` 的 `producer` 节点，并在本地用 `tornado` 轮询每个连接，当某个连接有可读事件时，即有消息达到，处理即可。

**负载均衡** - 当向某个 `Topic` 发布一个消息时，该消息会被复制到所有的 `Channel`，如果 `Channel` 只有一个客户端，那么 `Channel` 就将消息投递给这个客户端；如果 `Channel` 的客户端不止一个，那么 `Channel` 将把消息随机投递给任何一个客户端，这也可以看做是客户端的负载均衡；

### 6.4 最小堆 - 优先级队列

**优先级队列（ `Priority Queue` ）** - 通过数据结构最小堆（ `min heap` ）实现，`pub` 一条消息时立即就排好序（优先级通过 `Priority-timeout` 时间戳排序），最近到期的放到最小堆根节点；取出一条消息直接从最小堆的根节点取出，时间复杂度很低。

```go
type Item struct {
  Value    interface{}
  Priority int64
  Index    int
}

// this is a priority queue as implemented by a min heap
// ie. the 0th element is the *lowest* value
type PriorityQueue []*Item
```

### 6.5 队列设计 - 延时/运行队列

**延时队列（ `deferredPQ` ）** - 通过 `DPUB` 发布消息时带有 `timeout` 属性值实现，表示从当前时间戳多久后可以取出来消费；

**运行队列（ `inFlightPQ` ）** - 正在被消费者 `consumer` 消费的消息放入运行队列中，若处理失败或超时则自动重新放入（ `Requeue` ）队列，待下一次取出再次消费；消费成功（ `Finish` ）则删除对应的消息。

### 6.6 分布式 - 去中心化/无 SPOF

`nsq` 被设计以分布的方式被使用，客户端连接到指定 `topic` 的所有生产者 `producer` 实例。没有中间人，没有消息代理 `broker` ，也没有单点故障（ `SPOF - single point of failure` ）。
这种拓扑结构消除单链，聚合，消费者直接连接所有生产者。从技术上讲，哪个客户端连接到哪个 `nsq` 不重要，只要有足够的消费者 `consumer` 连接到所有生产者 `producer`，以满足大量的消息，保证所有东西最终将被处理。

对于 `nsqlookupd`，高可用性是通过运行多个实例来实现。他们不直接相互通信和数据被认为是最终一致。如果某个 `nsqd` 出现问题，`down` 机了，会和 `nsqlookupd` 断开，这样客户端从 `nsqlookupd` 得到的 `nsqd` 的列表永远是可用的。客户端连接的是所有的 `nsqd`，一个出问题了就用其他的连接，所以也不会受影响。

### 6.7 高可用、大吞吐量

**高可用性（ `HA` ）** - 通过集群化部署多个 `nsqd, nsqlookupd` 节点，可实现同时多生产者、多消费者运行，单一节点出现故障不影响系统运行；每个节点启动时都会先从磁盘读取未处理的消息，极端情况下，会丢失少量还未来得及存盘的内存中消息。

**10 亿/天** - 通过 `goroutine, channel` 充分利用 `golang` 语言的协程并发特性，可高并发处理大量消息的生产与消费。例如 `message` 为 `10 byte` 大小，则 50( `nsq` 节点数) \* 10(字节) * 86400(一天秒数) * 25(每秒处理消息数) = 10 亿，可见达到十亿级别的吞吐量，通过快速部署节点即可实现。

### 6.8 协议规范

自定义 `protocol`、魔法字符串 `magicStr` 进行通信、版本控制：

**通信协议**

nsqd

> FIN - 消息消费完成  
> RDY - 客户端连接就绪  
> REQ - 消息重放入队  
> PUB - 发布一条消息  
> MPUB - 发布多条消息  
> DPUB - 发布一条延时消息  
> NOP - 空操作  
> TOUCH - 重置消息过期时间  
> SUB - 消费者订阅 `Topic/Channel`  
> CLS - 超时关闭连接`CLOSE_WAIT`  
> AUTH - 权限认证

nsqlookupd

> PING - 心跳检测  
> IDENTIFY - 权限与协议校验  
> REGISTER - `nsqd`节点注册  
> UNREGISTER - `nsqd`节点注销

**版本控制**

> nsqd - " V2" (4 byte)  
> nsqlookupd - " V1" (4 byte)

### 6.9 快速扩缩容

`nsq` 集群很容易配置（多种参数设定方式：命令行 > 配置文件 > 默认值）和部署（编译的二进制可执行文件没有运行时依赖），通过简单设置初始化参数，运维 `Ops` 就可以快速增加 `nsqd` 或 `nsqlookupd` 节点，为 `Topic` 引入一个新的消费者，只需启动一个配置了 `nsqlookup` 实例地址的 `nsq` 客户端。无需为添加任何新的消费者或生产者更改配置，大大降低了开销和复杂性。

通过容器化管理多个实例将非常快速进行生产者、消费者的扩缩容，加上容器的流量监控、熔断、最低节点数等功能，保证了集群中 `nsqd` 的高效运行。

## 7. 小结

从源码可以看到，`nsqd` 的作用就是实际干活的组件，生产者 `producer`、消费者 `consumer` 利用 `nsqlookupd` 获取最新可用的节点，当连接上对应的 `Topic/Channel` 后，将消息 `message` 发送到客户端进行消费，处理成功则 `FIN(finish)`，或失败/超时后重新放回队列 `REQ(requeue)`，待下一次再消费处理。`nsqlookupd` 的作用就是管理 `nsqd` 节点的认证、注册、注销、心跳检测，动态维护分布式集群中最新可用的 `nsqd` 节点列表供客户端取用。

在可靠性、有序性方便， `nsq` 保证消息至少被投递消费一次（幂等消费），当某个 `nsqd` 节点出现故障时，极端情况下内存里面的消息还未来得及存入磁盘，这部分消息将丢失；通过分布式多个 `consumer` 消费，会因为消息处理时长、网络延迟等导致消息重排，再次消费顺序与写入顺序不一致，因此在高可靠性、顺序性方面略存在不足，应根据具体的业务场景进行取舍。

**综上：** 源代码实现逻辑清晰明了，源码中使用了很多读写锁 `RWMutex`、原子值 `atomic.Value`、`interface` 接口复用、自定义通信协议 `protocol`、`http-decorator`装饰器、`goroutine/channel` 协程间并发通信，优先从内存（ `msqChan` ）存取消息，从而保证了高可用、高吞吐量的应用能力。快速高效的节点配置与扩展，配合容器云编排技术，可以高效实现集群的 scale 化。

### 参考资料

- [nsq 官方文档](https://nsq.io/overview/design.html)
- [常用消息队列介绍和对比](https://www.cnblogs.com/xifengxiaoma/p/9391647.html)
- [nsq 去中心化原理](https://juejin.im/post/5d68cce2f265da039d32e39e)
- [NSQ 源码分析之概述](http://luodw.cc/2016/12/08/nsq01/#more)


