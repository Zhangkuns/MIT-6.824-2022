# 实验3：容错键/值服务
- 在这个实验中，你将使用Lab 2中的`Raft`库构建一个具有容错性的键/值存储服务（`key/value service`）。你的键/值服务将是一个复制状态机，由多个使用 Raft 进行复制的键/值服务器（`key/value servers`）组成。你的键/值服务能够在大多数服务器仍然存活且能够通信的情况下继续处理客户端请求，即使发生其他故障或网络分区。在Lab 3之后，你将实现`Raft`相互作用图中显示的所有部分（`Clerk`、`Service`和`Raft`）。

- 客户端可以向键/值服务发送三种不同的RPC：
  1. `Put(key, value)`
  2. `Append(key, arg)`
  3. `Get(key)`

- 服务维护一个简单的键/值对数据库，键和值都是字符串。`Put(key, value)`替换数据库中特定键的值，`Append(key, arg)`将`arg`追加到键的值，`Get(key)`获取键的当前值。对于不存在的键的`Get`应该返回空字符串。对不存在的键的`Append`应该像`Put`一样操作。每个客户端通过具有`Put/Append/Get`方法的`Clerk`与`service`进行通信。`Clerk`管理与`servers`的`RPC`交互。

- 你的服务必须安排应用程序调用`Clerk`的`Get/Put/Append`方法是可线性化的。如果一次调用一个，则 `Get/Put/Append` 方法应该表现为系统只有一份其状态副本，并且每次调用都应该观察前面的调用序列所隐含的状态修改。对于并发调用，返回值和最终状态必须与以某种顺序逐个执行操作时相同。如果调用在时间上重叠，则调用是并发的：例如，如果客户端 X 调用`Clerk.Put()`，客户端 Y 调用`Clerk.Append()`，然后客户端 X 的调用返回。调用必须观察到已完成的所有调用的效果，然后调用开始。

- 线性化对于应用程序来说很方便，因为这是你从一次处理一个请求的单个服务器中看到的行为。例如，如果一个客户端对服务的更新请求获得了成功的响应，随后启动的其他客户端的读取将确保看到更新后的效果。对于单个服务器而言，提供可线性化相对容易。如果服务被复制，那么提供可线性化就会更加困难，因为所有服务器必须为并发请求选择相同的执行顺序，必须避免使用不是最新的状态回复客户端，并且在故障后必须以一种保留所有已确认客户端更新的方式恢复其状态。

- 这个实验分为两个部分。在Part A，你将使用Raft实现一个键/值服务，但不使用快照。在Part B，你将使用Lab 2D中的快照实现，该实现允许`Raft`丢弃旧的日志条目。请在各自的截止日期前提交每个部分。

- 你应该仔细阅读扩展的`Raft`论文，特别是第7和第8节。为了获得更广泛的视角，请查看Chubby、Paxos Made Live、Spanner、Zookeeper、Harp、Viewstamped Replication和Bolosky等。

- 请尽早开始。
## 入门指南
- 我们为你提供了`src/kvraft`中的骨架代码和测试。你需要修改`kvraft/client.go`、`kvraft/server.go`，以及可能的情况下修改`kvraft/common.go`。

- 要开始运行，请执行以下命令。不要忘记使用`git pull`获取最新的软件。
```shell
$ cd ~/6.824
$ git pull
...
$ cd src/kvraft
$ go test -race
...
$
```
## Part A：无快照的键/值服务（中等/困难）

- 每个键/值服务器（`kvservers`）都将有一个关联的 `Raft`对等体。`Clerk`向其关联的`Raft`是领导者的`kvserver`发送`Put()、Append()和Get()` RPC。`kvserver`代码将`Put/Append/Get`操作提交给 Raft，以便 Raft 日志保存一系列`Put/Append/Get`操作。所有`kvservers`按顺序执行 Raft 日志中的操作，将操作应用于其键/值数据库；其目的是使服务器保持键/值数据库的相同副本。

- 有时`Clerk` 不知道哪个`kvserver`是`Raft`的领导者。如果`Clerk` 向错误的`kvserver`发送`RPC`，或者无法联系`kvserver`，`Clerk`应通过发送到另一个`kvserver`进行重试。如果键/值服务将操作提交到其 Raft 日志（因此将操作应用于键/值状态机），领导者通过响应其RPC向Clerk报告结果。如果操作未能提交（例如，如果领导者被替换），服务器报告错误，Clerk使用不同的服务器重试。

- 你的`kvservers`不应直接通信；它们应只通过 Raft 相互交互。
### Task
- 你的第一个任务是在没有丢弃消息和没有故障服务器的情况下实现一个解决方案。

- 你需要在`client.go`的`Clerk Put/Append/Get`方法中添加RPC发送代码，并在`server.go`中实现`PutAppend()`和`Get()` RPC处理程序。这些处理程序应使用`Start()`在`Raft`日志中输入一个`Op`；你应该在`server.go`中填充`Op`结构的定义，以便描述`Put/Append/Get`操作。每个服务器应在`Raft`提交它们时执行`Op`命令，即它们出现在`applyCh`上。当`Raft`提交其`Op`时，`RPC`处理程序应注意到，并回复`RPC`。

- 当你能够可靠通过测试套件中的第一个测试：“一个客户端”时，你已经完成了此任务。
> task end
### Hint
- 调用`Start()`之后，你的`kvservers`将需要等待`Raft`完成协议。已经达成一致的命令将到达`applyCh`。你的代码将需要在`PutAppend()`和`Get()`处理程序使用`Start()`向`Raft`日志提交命令时保持读取`applyCh`。要注意在`kvserver`和其`Raft`库之间避免死锁。

- 你被允许向`Raft`的`ApplyMsg`添加字段，并向`Raft`的`RPC`（如`AppendEntries`）添加字段，但对于大多数实现而言，这通常是不必要的。

- 如果`kvserver`不是大多数的一部分，它不应完成`Get() RPC`（以便它不提供过时的数据）。一个简单的解决方案是将每个`Get()`（以及每个`Put()`和`Append()`）输入`Raft`日志。你不必实现第8节中描述的只读操作的优化。

- 最好从一开始就添加锁定，因为需要避免死锁有时会影响整体代码设计。使用`go test -race`检查你的代码是否没有竞态条件。
> hint end

- 现在，你应该修改你的解决方案，以应对网络和服务器故障。你将面临的一个问题是，`Clerk`可能必须多次发送 `RPC`，直到找到一个回复正面的`kvserver`。如果领导者在将条目提交到 `Raft` 日志后失败，`Clerk`可能不会收到回复，因此可能会将请求重新发送到另一个领导者。每次调用`Clerk.Put()`或`Clerk.Append()`应该仅导致一次执行，因此你必须确保重新发送不会导致服务器执行请求两次。
### Task
- 添加处理故障的代码，并处理重复的`Clerk`请求，包括`Clerk`在一个任期内将请求发送到`kvserver`领导者，等待回复超时，然后在另一个任期内将请求重新发送到新领导者的情况。请求应该只执行一次。你的代码应通过`go test -run 3A -race`测试。
> task end

### Hint
- 你的解决方案需要处理一个已经调用了`Start()`方法`Clerk`的`RPC`的`leader`，但在请求被提交到日志之前失去了领导权的情况。在这种情况下，你应该安排`Clerk`将请求重新发送到其他服务器，直到找到新的`leader`。一种方法是让服务器检测到它已经失去了领导权，方法是注意到在`Start()`返回的索引处出现了不同的请求，或者`Raft`的`term`已经改变。如果前领导者被隔离，它将不知道新的领导者；但是，同一分区中的任何客户端也无法与新的领导者通信，因此在这种情况下，服务器和客户端可以无限期地等待分区恢复。

- 你可能需要修改你的`Clerk`，以记住哪个服务器最终成为了上一个 `RPC` 的`leader`，并首先将下一个`RPC`发送到该服务器。这将避免在每个`RPC` 上搜索`leader`，从而可能帮助你快速通过一些测试.

- 你需要唯一地标识客户端操作，以确保键/值服务只执行每个操作一次。

- 你的重复检测方案应该快速释放服务器内存，例如每个`RPC`都意味着客户端已经看到了其前一个`RPC`的回复。可以假设客户端一次只会向`Clerk`发出一个调用.
> hint end

- 你的代码现在应该能够通过Lab 3A测试，就像这样：
```shell
$ go test -run 3A -race
Test: one client (3A) ...
  ... Passed --  15.5  5  4576  903
Test: ops complete fast enough (3A) ...
  ... Passed --  15.7  3  3022    0
Test: many clients (3A) ...
  ... Passed --  15.9  5  5884 1160
Test: unreliable net, many clients (3A) ...
  ... Passed --  19.2  5  3083  441
Test: concurrent append to same key, unreliable (3A) ...
  ... Passed --   2.5  3   218   52
Test: progress in majority (3A) ...
  ... Passed --   1.7  5   103    2
Test: no progress in minority (3A) ...
  ... Passed --   1.0  5   102    3
Test: completion after heal (3A) ...
  ... Passed --   1.2  5    70    3
Test: partitions, one client (3A) ...
  ... Passed --  23.8  5  4501  765
Test: partitions, many clients (3A) ...
  ... Passed --  23.5  5  5692  974
Test: restarts, one client (3A) ...
  ... Passed --  22.2  5  4721  908
Test: restarts, many clients (3A) ...
  ... Passed --  22.5  5  5490 1033
Test: unreliable net, restarts, many clients (3A) ...
  ... Passed --  26.5  5  3532  474
Test: restarts, partitions, many clients (3A) ...
  ... Passed --  29.7  5  6122 1060
Test: unreliable net, restarts, partitions, many clients (3A) ...
  ... Passed --  32.9  5  2967  317
Test: unreliable net, restarts, partitions, random keys, many clients (3A) ...
  ... Passed --  35.0  7  8249  746
PASS
ok  	6.824/kvraft	290.184s
```
- 每个“Passed”后面的数字是实际时间（以秒为单位），对等体数量，发送的 RPC 数量（包括客户端 RPC ）以及执行的键/值操作数量（`Clerk Get/Put/Append`调用）。
- 目前，你的键/值服务器没有调用 Raft 库的`Snapshot()`方法，因此重新启动的服务器必须重放完整的持久性 Raft 日志以恢复其状态。现在，你将修改`kvserver`以与 Raft 合作，使用`Lab 2D`中的 Raft 的 `Snapshot()`来保存日志空间并减少重新启动时间。

## Part B: 带快照的键/值服务（困难）
- 测试程序将`maxraftstate`传递给你的`StartKVServer()`。`maxraftstate`指示持久`Raft`状态的最大允许大小（以字节为单位，包括日志，但不包括快照）。你应该将`maxraftstate`与`persister.RaftStateSize()`进行比较。每当你的键/值服务器检测到`Raft`状态大小接近此阈值时，它应该通过调用`Raft`的`Snapshot()`保存一个快照。如果`maxraftstate`为`-1`，则无需快照。`maxraftstate`适用于`Raft` 传递给`persister.SaveRaftState()`的GOB编码字节。
### Task
- 修改你的`kvserver`，使其在持久化`Raft`状态增长过大时检测到，并然后传递一个快照给`Raft`。当`kvserver`服务器重新启动时，它应该从`persister`读取快照并从快照中恢复其状态。
> task end
### Hint
- 考虑一下`kvserver`应该什么时候对其状态进行快照，以及快照应该包含什么。`Raft`使用`SaveStateAndSnapshot()`将每个快照与相应的`Raft`状态存储在`persister`对象中。你可以使用`ReadSnapshot()`读取最新存储的快照。

- 你的`kvserver`必须能够检测到跨检查点的日志中的重复操作，因此用于检测它们的任何状态必须包含在快照中。

- 将快照中存储的结构的所有字段大写。

- 你的`Raft`库可能存在此实验中暴露的错误。如果对`Raft`实现进行更改，请确保它继续通过所有Lab 2的测试。

- Lab 3测试所需的合理时间为实际时间400秒和CPU时间700秒。此外，`go test -run TestSnapshotSize`应该在实际时间少于20秒的情况下完成。
> hint end
- 你的代码应该通过3B测试（如此处的示例）以及3A测试（并且你的`Raft`必须继续通过Lab 2的测试）。
```shell
$ go test -run 3B -race
Test: InstallSnapshot RPC (3B) ...
  ... Passed --   4.0  3   289   63
Test: snapshot size is reasonable (3B) ...
  ... Passed --   2.6  3  2418  800
Test: ops complete fast enough (3B) ...
  ... Passed --   3.2  3  3025    0
Test: restarts, snapshots, one client (3B) ...
  ... Passed --  21.9  5 29266 5820
Test: restarts, snapshots, many clients (3B) ...
  ... Passed --  21.5  5 33115 6420
Test: unreliable net, snapshots, many clients (3B) ...
  ... Passed --  17.4  5  3233  482
Test: unreliable net, restarts, snapshots, many clients (3B) ...
  ... Passed --  22.7  5  3337  471
Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...
  ... Passed --  30.4  5  2725  274
Test: unreliable net, restarts, partitions, snapshots, random keys, many clients (3B) ...
  ... Passed --  37.7  7  8378  681
PASS
ok  	6.824/kvraft	161.538s
```
