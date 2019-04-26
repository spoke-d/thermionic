package daemon

import (
	"context"
	"crypto/x509"
	"net/http"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster"
	"github.com/spoke-d/thermionic/internal/cluster/heartbeat"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/upgraded"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	querycluster "github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/discovery"
	"github.com/spoke-d/thermionic/internal/schedules"
	"github.com/spoke-d/thermionic/internal/state"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/spoke-d/thermionic/pkg/events"
)

type membershipStateShim struct {
	state *state.State
}

func makeMembershipStateShim(state *state.State) membershipStateShim {
	return membershipStateShim{
		state: state,
	}
}

func (s membershipStateShim) Node() membership.Node {
	return s.state.Node()
}

func (s membershipStateShim) Cluster() membership.Cluster {
	return s.state.Cluster()
}

func (s membershipStateShim) OS() membership.OS {
	return s.state.OS()
}

type upgradedStateShim struct {
	state *state.State
}

func makeUpgradedStateShim(state *state.State) upgradedStateShim {
	return upgradedStateShim{
		state: state,
	}
}

func (s upgradedStateShim) Node() upgraded.Node {
	return s.state.Node()
}

func (s upgradedStateShim) Cluster() upgraded.Cluster {
	return s.state.Cluster()
}

func (s upgradedStateShim) OS() upgraded.OS {
	return s.state.OS()
}

type daemonShim struct {
	daemon *Daemon
}

func makeDaemonShim(daemon *Daemon) daemonShim {
	return daemonShim{
		daemon: daemon,
	}
}

func (s daemonShim) Cluster() api.Cluster {
	return s.daemon.Cluster()
}

func (s daemonShim) Node() api.Node {
	return s.daemon.Node()
}

func (s daemonShim) State() api.State {
	return makeAPIStateShim(s.daemon.State())
}

func (s daemonShim) Gateway() api.Gateway {
	return makeAPIGatewayShim(s.daemon.Gateway())
}

func (s daemonShim) Operations() api.Operations {
	return s.daemon.Operations()
}

func (s daemonShim) Schedules() api.Schedules {
	return s.daemon.Schedules()
}

func (s daemonShim) ClientCerts() []x509.Certificate {
	return s.daemon.ClientCerts()
}

func (s daemonShim) ClusterCerts() []x509.Certificate {
	return s.daemon.ClusterCerts()
}

func (s daemonShim) ClusterConfigSchema() config.Schema {
	return s.daemon.ClusterConfigSchema()
}

func (s daemonShim) NodeConfigSchema() config.Schema {
	return s.daemon.NodeConfigSchema()
}

func (s daemonShim) SetupChan() <-chan struct{} {
	return s.daemon.SetupChan()
}

func (s daemonShim) RegisterChan() <-chan struct{} {
	return s.daemon.RegisterChan()
}

func (s daemonShim) ActorGroup() api.ActorGroup {
	return makeAPIActorGroupShim(s.daemon.ActorGroup())
}

func (s daemonShim) EventBroadcaster() api.EventBroadcaster {
	return s.daemon.EventBroadcaster()
}

func (s daemonShim) Version() string {
	return s.daemon.Version()
}

func (s daemonShim) Nonce() string {
	return s.daemon.Nonce()
}

func (s daemonShim) Endpoints() api.Endpoints {
	return s.daemon.Endpoints()
}

func (s daemonShim) APIExtensions() []string {
	return s.daemon.APIExtensions()
}

func (s daemonShim) APIMetrics() api.Metrics {
	return makeAPIMetricsShim(s.daemon.APIMetrics())
}

func (s daemonShim) UnsafeShutdown() {
	s.daemon.UnsafeShutdown()
}

func (s daemonShim) UnsafeSetCluster(cluster api.Cluster) {
	s.daemon.UnsafeSetCluster(cluster)
}

type apiMetricsShim struct {
	apiMetrics APIMetrics
}

func makeAPIMetricsShim(apiMetrics APIMetrics) apiMetricsShim {
	return apiMetricsShim{
		apiMetrics: apiMetrics,
	}
}

func (s apiMetricsShim) APIDuration() api.HistogramVec {
	return makeHistogramVecShim(s.apiMetrics.APIDuration())
}

func (s apiMetricsShim) ConnectedClients() api.GaugeVec {
	return makeGaugeVecShim(s.apiMetrics.ConnectedClients())
}

type histogramVecShim struct {
	histogramVec HistogramVec
}

func makeHistogramVecShim(histogramVec HistogramVec) histogramVecShim {
	return histogramVecShim{
		histogramVec: histogramVec,
	}
}

func (s histogramVecShim) WithLabelValues(lvs ...string) api.Histogram {
	return s.histogramVec.WithLabelValues(lvs...)
}

type guageVecShim struct {
	guageVec GaugeVec
}

func makeGaugeVecShim(guageVec GaugeVec) guageVecShim {
	return guageVecShim{
		guageVec: guageVec,
	}
}

func (s guageVecShim) WithLabelValues(lvs ...string) api.Gauge {
	return s.guageVec.WithLabelValues(lvs...)
}

type heartbeatGatewayShim struct {
	gateway Gateway
}

func makeHeartbeatGatewayShim(gateway Gateway) heartbeatGatewayShim {
	return heartbeatGatewayShim{
		gateway: gateway,
	}
}

func (s heartbeatGatewayShim) RaftNodes() ([]db.RaftNode, error) {
	return s.gateway.RaftNodes()
}

func (s heartbeatGatewayShim) DB() heartbeat.Node {
	return s.gateway.DB()
}

func (s heartbeatGatewayShim) Cert() *cert.Info {
	return s.gateway.Cert()
}

func (s heartbeatGatewayShim) Clustered() bool {
	return s.gateway.Clustered()
}

type eventsActorGroupShim struct {
	actorGroup ActorGroup
}

func makeEventsActorGroupShim(actorGroup ActorGroup) eventsActorGroupShim {
	return eventsActorGroupShim{
		actorGroup: actorGroup,
	}
}

func (s eventsActorGroupShim) Add(a events.Actor) {
	s.actorGroup.Add(a)
}

func (s eventsActorGroupShim) Prune() bool {
	return s.actorGroup.Prune()
}

func (s eventsActorGroupShim) Walk(fn func(events.Actor) error) error {
	return s.actorGroup.Walk(func(a Actor) error {
		return fn(a)
	})
}

type apiActorGroupShim struct {
	actorGroup ActorGroup
}

func makeAPIActorGroupShim(actorGroup ActorGroup) apiActorGroupShim {
	return apiActorGroupShim{
		actorGroup: actorGroup,
	}
}

func (s apiActorGroupShim) Add(a api.Actor) {
	s.actorGroup.Add(a)
}

func (s apiActorGroupShim) Prune() bool {
	return s.actorGroup.Prune()
}

func (s apiActorGroupShim) Walk(fn func(api.Actor) error) error {
	return s.actorGroup.Walk(func(a Actor) error {
		return fn(a)
	})
}

type apiStateShim struct {
	state *state.State
}

func makeAPIStateShim(state *state.State) apiStateShim {
	return apiStateShim{
		state: state,
	}
}

func (s apiStateShim) Cluster() api.Cluster {
	return s.state.Cluster()
}

func (s apiStateShim) Node() api.Node {
	return s.state.Node()
}

func (s apiStateShim) OS() api.OS {
	return s.state.OS()
}

type apiGatewayShim struct {
	gateway Gateway
}

func makeAPIGatewayShim(gateway Gateway) apiGatewayShim {
	return apiGatewayShim{
		gateway: gateway,
	}
}

func (s apiGatewayShim) Init(certInfo *cert.Info) error {
	return s.gateway.Init(certInfo)
}

func (s apiGatewayShim) HandlerFuncs() map[string]http.HandlerFunc {
	return s.gateway.HandlerFuncs()
}

func (s apiGatewayShim) Shutdown() error {
	return s.gateway.Shutdown()
}

func (s apiGatewayShim) WaitLeadership() error {
	return s.gateway.WaitLeadership()
}

func (s apiGatewayShim) RaftNodes() ([]db.RaftNode, error) {
	return s.gateway.RaftNodes()
}

func (s apiGatewayShim) Raft() api.RaftInstance {
	return s.gateway.Raft()
}

func (s apiGatewayShim) DB() api.Node {
	return s.gateway.DB()
}

func (s apiGatewayShim) IsDatabaseNode() bool {
	return s.gateway.IsDatabaseNode()
}

func (s apiGatewayShim) Cert() *cert.Info {
	return s.gateway.Cert()
}

func (s apiGatewayShim) LeaderAddress() (string, error) {
	return s.gateway.LeaderAddress()
}

func (s apiGatewayShim) Reset(certInfo *cert.Info) error {
	return s.gateway.Reset(certInfo)
}

func (s apiGatewayShim) DialFunc() dqlite.DialFunc {
	return s.gateway.DialFunc()
}

func (s apiGatewayShim) ServerStore() querycluster.ServerStore {
	return s.gateway.ServerStore()
}

func (s apiGatewayShim) Context() context.Context {
	return s.gateway.Context()
}

type daemonGatewayShim struct {
	gateway *cluster.Gateway
}

func makeDaemonGatewayShim(gateway *cluster.Gateway) daemonGatewayShim {
	return daemonGatewayShim{
		gateway: gateway,
	}
}

func (s daemonGatewayShim) Init(certInfo *cert.Info) error {
	return s.gateway.Init(certInfo)
}

func (s daemonGatewayShim) HandlerFuncs() map[string]http.HandlerFunc {
	return s.gateway.HandlerFuncs()
}

func (s daemonGatewayShim) Shutdown() error {
	return s.gateway.Shutdown()
}

func (s daemonGatewayShim) WaitLeadership() error {
	return s.gateway.WaitLeadership()
}

func (s daemonGatewayShim) WaitUpgradeNotification() {
	s.gateway.WaitUpgradeNotification()
}

func (s daemonGatewayShim) RaftNodes() ([]db.RaftNode, error) {
	return s.gateway.RaftNodes()
}

func (s daemonGatewayShim) Raft() RaftInstance {
	return s.gateway.Raft()
}

func (s daemonGatewayShim) DB() Node {
	return s.gateway.DB()
}

func (s daemonGatewayShim) Clustered() bool {
	return s.gateway.Clustered()
}

func (s daemonGatewayShim) IsDatabaseNode() bool {
	return s.gateway.IsDatabaseNode()
}

func (s daemonGatewayShim) Cert() *cert.Info {
	return s.gateway.Cert()
}

func (s daemonGatewayShim) LeaderAddress() (string, error) {
	return s.gateway.LeaderAddress()
}

func (s daemonGatewayShim) Reset(certInfo *cert.Info) error {
	return s.gateway.Reset(certInfo)
}

func (s daemonGatewayShim) DialFunc() dqlite.DialFunc {
	return s.gateway.DialFunc()
}

func (s daemonGatewayShim) ServerStore() querycluster.ServerStore {
	return s.gateway.ServerStore()
}

func (s daemonGatewayShim) Context() context.Context {
	return s.gateway.Context()
}

func (s daemonGatewayShim) Kill() {
	s.gateway.Kill()
}

type daemonSchedulesShim struct {
	daemon *Daemon
}

func makeDaemonSchedulesShim(daemon *Daemon) daemonSchedulesShim {
	return daemonSchedulesShim{
		daemon: daemon,
	}
}

func (s daemonSchedulesShim) Cluster() schedules.Cluster {
	return s.daemon.Cluster()
}

func (s daemonSchedulesShim) Node() schedules.Node {
	return s.daemon.Node()
}

func (s daemonSchedulesShim) Gateway() schedules.Gateway {
	return makeAPIGatewayShim(s.daemon.Gateway())
}

func (s daemonSchedulesShim) NodeConfigSchema() config.Schema {
	return s.daemon.NodeConfigSchema()
}

type daemonDiscoveryShim struct {
	daemon *Daemon
}

func makeDaemonDiscoveryShim(daemon *Daemon) daemonDiscoveryShim {
	return daemonDiscoveryShim{
		daemon: daemon,
	}
}

func (s daemonDiscoveryShim) Cluster() discovery.Cluster {
	return s.daemon.Cluster()
}

func (s daemonDiscoveryShim) Node() discovery.Node {
	return s.daemon.Node()
}

func (s daemonDiscoveryShim) Gateway() discovery.Gateway {
	return makeAPIGatewayShim(s.daemon.Gateway())
}

func (s daemonDiscoveryShim) NodeConfigSchema() config.Schema {
	return s.daemon.NodeConfigSchema()
}

func (s daemonDiscoveryShim) Endpoints() discovery.Endpoints {
	return s.daemon.Endpoints()
}
