package raft

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

// RaftMetrics 封装所有 Prometheus 指标
type RaftMetrics struct {
	// 节点状态指标
	currentTerm prometheus.Gauge
	state       prometheus.Gauge
	votedFor    prometheus.Gauge

	// 日志相关指标
	logSize     prometheus.Gauge
	commitIndex prometheus.Gauge
	lastApplied prometheus.Gauge

	// 日志复制指标
	replicationLag *prometheus.GaugeVec

	// 选举相关计数器
	electionStarted prometheus.Counter // 发起选举的次数
	becameLeader    prometheus.Counter // 成为Leader的次数
	becameFollower  prometheus.Counter // 成为Follower的次数
	votesReceived   prometheus.Counter // 收到的选票数

	// RPC 相关指标
	rpcRequestTotal *prometheus.CounterVec
	rpcLatency      *prometheus.HistogramVec

	// 心跳指标
	heartbeatsSent     prometheus.Counter
	heartbeatsReceived prometheus.Counter

	// 定时器相关
	electionTimeout  prometheus.Gauge // 当前的选举超时设置
	heartbeatTimeout prometheus.Gauge // 当前的心跳超时设置
}

func NewRaftMetrics(nodeID int) *RaftMetrics {
	metrics := &RaftMetrics{
		// 节点状态指标
		currentTerm: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "raft_current_term",
			Help:        "Current term number",
			ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
		}),

		state: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "raft_state",
			Help:        "Node state (0=Follower, 1=Candidate, 2=Leader)",
			ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
		}),

		votedFor: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "raft_voted_for",
			Help:        "Node voted for in current term (-1 means not voted)",
			ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
		}),

		// 选举相关计数器
		electionStarted: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "raft_elections_started_total",
			Help:        "Total number of elections started",
			ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
		}),

		becameLeader: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "raft_became_leader_total",
			Help:        "Total number of times became leader",
			ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
		}),

		becameFollower: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "raft_became_follower_total",
			Help:        "Total number of times became follower",
			ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
		}),

		votesReceived: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "raft_votes_received_total",
			Help:        "Total number of votes received",
			ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
		}),

		// RPC 相关指标
		rpcRequestTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "raft_rpc_requests_total",
				Help:        "Total number of RPC requests",
				ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
			},
			[]string{"type"}, // RequestVote or AppendEntries
		),

		rpcLatency: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        "raft_rpc_latency_seconds",
				Help:        "RPC latency distributions",
				Buckets:     []float64{.001, .005, .01, .025, .05, .1, .25, .5},
				ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
			},
			[]string{"type"},
		),

		// 心跳相关指标
		heartbeatsSent: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "raft_heartbeats_sent_total",
			Help:        "Total number of heartbeats sent",
			ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
		}),

		heartbeatsReceived: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "raft_heartbeats_received_total",
			Help:        "Total number of heartbeats received",
			ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
		}),

		// 定时器相关指标
		electionTimeout: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "raft_election_timeout_ms",
			Help:        "Current election timeout in milliseconds",
			ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
		}),

		heartbeatTimeout: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "raft_heartbeat_timeout_ms",
			Help:        "Current heartbeat timeout in milliseconds",
			ConstLabels: prometheus.Labels{"node": fmt.Sprintf("%d", nodeID)},
		}),
	}

	// 启动 metrics http server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(fmt.Sprintf(":%d", 9090+nodeID), nil)
	}()

	return metrics
}
