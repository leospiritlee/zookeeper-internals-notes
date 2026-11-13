# 第7章 请求处理链：一次写请求穿过哪些处理器

**一句话**：ZooKeeper 把每台服务器实现成一条「请求处理器流水线」，请求依次流过所有处理器才算被完全处理；写请求的落盘、提议、确认与最终应用，分别挂在不同的处理器上。

**为什么需要它**：前面几章讲的是协议消息（`PROPOSAL` / `ACK` / `COMMIT`），但消息不会自己发出去。语料第 9 章「服务器的构成」给了实现侧的抽象：群首、追随者和观察者根本上都是服务器，主要抽象概念是**请求处理器**，它是对处理流水线上不同阶段的抽象；每一个服务器实现一个请求处理器的序列，一条请求经过流水线上所有处理器后才被称为得到完全处理。看懂这条链，才能把「一次写请求为什么慢」「追随者为什么不会把读请求提前执行」这类问题落到具体环节上。

**核心结构**：

| 服务器 | 语料中的流水线 | 关键分工 |
|---|---|---|
| 独立服务器 | `PrepRequestProcessor` → `SyncRequestProcessor` → `FinalRequestProcessor` | 三段完成「生成事务 → 持久化 → 应用」 |
| 群首 | `PrepRequestProcessor` → `ProposalRequestProcessor` → `CommitRequestProcessor`（所有请求）/ `SyncRequestProcessor`（写请求）→ `AckRequestProcessor` → `FinalRequestProcessor` | 准备提议、发给追随者、持久化并确认 |
| 追随者 | `FollowerRequestProcessor` → `CommitRequestProcessor`，写请求同时转发给群首 | 输入有客户端请求、提议、提交事务三种 |
| 观察者 | `ObserverZooKeeperServer`，流水线与追随者非常相似 | 不确认提议、不持久化事务到硬盘 |
| 公共类 | `ZooKeeperServer`、`ZKDatabase` | 独立服务器实现 / 数据树与持久化 |

**关键设计**：**把「生成事务」和「持久化事务」拆成两个处理器**。语料说 `PrepRequestProcessor` 接受客户端的请求并执行这个请求，处理结果是生成一个事务；事务是执行一个操作的结果，该操作会反映到 ZooKeeper 的数据树上，事务信息以头部形式记录。而 `SyncRequestProcessor` 负责把事务持久化到磁盘上。拆分之后，同一份事务既能被独立服务器直接落盘，也能被群首先拿去发提议、再落盘、再确认——**协议形态变了，但「事务」这个中间产物不变**，这正是三种服务器能共用大部分处理器的原因。

```text
【三条流水线（语料 9.5 口径）】
  独立服务器
    Prep ──► Sync(落盘) ──► Final(应用到数据树)

  群首 Leader
    Prep ──► Proposal ──┬──► Commit ──► Final
                        └──► Sync(写请求) ──► Ack

  追随者 Follower
    FollowerRequest ──┬──► Commit ──► Final
                      └──► 写请求转发给群首

  观察者 Observer：同追随者，但不 ACK、不落盘
```

**拆解**：

### 独立服务器：最短的一条链

语料说 ZooKeeper 中最简单的流水线是独立服务器（`ZooKeeperServer` 类），它包含三种请求处理器：`PrepRequestProcessor`、`SyncRequestProcessor` 和 `FinalRequestProcessor`。这条链把一次写请求的三件事排得清清楚楚：先把请求变成事务，再把事务持久化到磁盘，最后由 `FinalRequestProcessor` 把事务应用到数据树上并返回结果。它没有 `PROPOSAL`、没有 `ACK`、没有 `COMMIT`——因为独立模式下状态根本不复制（第 1 章）。**把这条链记住，另外两条链就是在它上面插入了「提议 / 确认」两步**。

### 群首：提议、落盘、确认各归其位

语料描述群首服务器流水线：第一个处理器同样是 `PrepRequestProcessor`，之后是 `ProposalRequestProcessor`，该处理器会准备一个提议并把提议发送给跟随者；`ProposalRequestProcessor` 会把所有请求都转发给 `CommitRequestProcessor`，而且对于写操作请求，还会把请求转发给 `SyncRequestProcessor`。`SyncRequestProcessor` 执行的操作与独立服务器中一样，即持久化事务到磁盘；执行完之后会触发 `AckRequestProcessor`。这里正好对应第 6 章那三步：**提议由 `ProposalRequestProcessor` 发出，追随者回的 `ACK` 最终由群首自己的 `AckRequestProcessor` 处理，过半之后 `COMMIT` 经 `CommitRequestProcessor` 落到 `FinalRequestProcessor`**。

### 追随者：三种输入、一条等待线

语料特别提醒，追随者（图中对应 `FollowerRequestProcessor` 类）「并不是一个单一序列的处理器，而且输入也有不同形式：客户端请求、提议、提交事务」。`FollowerRequestProcessor` 接收并处理客户端请求，之后转发请求给 `CommitRequestProcessor`，同时也会把写请求转发给群首服务器。还有一个容易被忽略的细节：语料说 `CommitRequestProcessor` 会让读取请求阻塞，直到把读取请求转给它；**通过等待的方式，请求可以被保证按照接收的顺序来执行**。这与第 6 章「追随者按照严格的顺序接收事务点」是同一条约束在实现侧的落地——读不能插到尚未提交的写前面去。

### 观察者：相似但少了两件事

语料说观察者服务器的请求流水线（`ObserverZooKeeperServer` 类）与追随者非常相似，但因为观察者不需要确认提议消息，所以它并不需要发送确认消息给群首服务器，也不用持久化事务到硬盘。语料还留了一句时代痕迹：关于观察者是否需要持久化事务到硬盘以加速恢复速度的讨论当时正在进行中。这条差异很重要：**观察者是读扩展，不是多一份副本**——它既不投票，也不落盘，因此加观察者不会提高法定人数、也不会提高可容忍的故障数（第 9 章展开）。

**今天怎么做**（延伸，非语料原文）：后续版本对请求处理器链有过重构与拆分，运维侧也更多通过指标与 JMX 观察各阶段队列长度，语料只给出了这条链的结构与 `SyncRequestProcessor.run()` 一处代码指引。**别用后来版本的处理器名字去替换语料里的类名**，本书关心的是分工——「生成事务 / 落盘 / 提议 / 确认 / 应用」这五个位置，版本换了位置仍在。

<div class="chapend" markdown="1">

**容易误解的地方**：
1. 「请求处理器是一条直线，所有服务器一样」——追随者的输入有三种（客户端请求、提议、提交事务），图里用箭头区分不同路径。
2. 「`FinalRequestProcessor` 负责写磁盘」——持久化在 `SyncRequestProcessor`，`FinalRequestProcessor` 是把事务应用到数据树。
3. 「群首自己不用落盘」——语料明确群首流水线上 `SyncRequestProcessor` 做的事与独立服务器一样，也是持久化事务到磁盘。
4. 「追随者收到读请求可以立刻返回本地数据」——`CommitRequestProcessor` 通过等待保证按接收顺序执行，读不能越过待提交的写。
5. 「观察者也存了一份完整数据副本」——语料明确它不确认提议、也不持久化事务到硬盘。
6. 「事务就是客户端发来的那条请求」——事务是执行请求的结果，会反映到数据树上，并带有头部信息（含 zxid）。

**记住这一句**：三种服务器共用「请求处理器流水线」这一个抽象，差别只在链上插了哪几步——独立服务器是「事务 → 落盘 → 应用」，群首多插「提议 / 落盘 / 确认」，追随者多插「转发写请求 / 等待保序」，观察者则省掉确认与落盘。

</div>
