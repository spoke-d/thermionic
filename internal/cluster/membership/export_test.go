package membership

import (
	"time"

	"github.com/spoke-d/thermionic/internal/db"
)

var MembershipCheckClusterStateForAccept = membershipCheckClusterStateForAccept

func ListStatus(l *List, nodes []db.NodeInfo, addresses []string, offlineThreshold time.Duration) ([]ClusterMember, error) {
	return l.status(nodes, addresses, offlineThreshold)
}

func JoinExitExclusive(j *Join, address string, operations []db.Operation) func(*db.ClusterTx) error {
	return j.exitExclusive(address, operations)
}
