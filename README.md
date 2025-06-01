
# Design Approach
Initially, the system appeared overwhelmingly complex due to numerous design details. To address this, I explored mature industrial projects for inspiration, primarily drawing from TiKV's architecture and common distributed system patterns to implement a simplified version.

## Transaction Model
Google's Percolator transaction model 
This model offers several advantages:

- Supports transactions spanning multiple shards.
- Uses two-phase commit for rollback capability.
- Multi-Version Concurrency Control (MVCC), storing key/value pairs with timestamps for isolation at specific points in time.

To support the Percolator model, introduced a Timestamp Oracle (TSO) server, which assigns unified timestamps to transactions.

## System Architecture
The overall workflow is as follows:
![[shardkv.png]]
Client: Queries the shard controller for the latest configuration, identifies the shard for a given key, and sends requests to the corresponding replica group.
Server: Periodically queries the shard controller to ensure configuration updates.
Client Transactions: Requests a timestamp from the TSO server for each transaction.
Shard Migration: Requests a timestamp from the TSO server during shard migrations.

### Shard Key/Value Storage (shardkv)
The shardkv storage is divided into three components, inspired by the Percolator model:
```
kvStore: map[string][]ValueVersion - Stores actual key/value pairs with version history.
locks: map[string]LockInfo - Manages locks for transaction coordination.
writeIntent: map[string]WriteIntentInfo - Tracks pending writes before commit.
```
Percolator Transaction Workflow
- Acquire Timestamp: Obtain a transaction start timestamp from the TSO server.
- Primary Key Locking: Select one key as the primary key, lock it, and check for conflicts. If the primary key conflicts, the transaction fails immediately. Other keys' locks point to the primary key.
- Write Intent: Record the intended writes in writeIntent.
- Commit: Validate locks and timestamps. If the primary key fails, roll back all changes.

### Operation Structure (Op)
The Op struct encapsulates all operations submitted to the Raft log for processing. 

The Op types include Read, Prewrite, Rollback, Commit, OpConfigChange, and OpApplyMigratedData. Operations are created via deep copy and submitted to an applyChannel to modify shardkv data, ensuring thread safety.

### Configuration Polling (pollConfig)
A dedicated pollConfig thread monitors configuration changes and submits OpConfigChange operations. Key details:

Only the Raft leader executes this thread.The thread periodically queries the shard controller for the latest configuration.

To handle network packet loss (observed in tests), I encapsulated configuration queries in a function with a timeout. If a query fails or times out, the thread retries after a delay.

### Shard Migration
Shard migration uses a push-based approach. After detecting a new configuration, groups with excess shards proactively transfer them to the designated groups. The receiving group updates its configuration and marks the shard's status as WaitingForData.

#### Shard Migration Workflow

- Pull Configuration: The group fetches the new configuration and submits an OpConfigChange.
- Apply Configuration: Raft applies the configuration, identifying shards to push or wait for data.
- Push Shards: A separate thread sends shards to the target group.
- Receive Shards: The receiving group submits an OpApplyMigratedData operation.
- Apply Data: The group applies the migrated data and updates the shard status.

To prevent configuration update conflicts, updates are only applied when all shards are in the Serving state, ensuring all migrated data has been received. 

