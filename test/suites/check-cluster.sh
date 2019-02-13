test_clustering_enable() {
    THERM_INIT_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_INIT_DIR}"
    spawn_therm "${TEST_DIR}" "${THERM_INIT_DIR}" 8090

    (
        set -e

        THERM_DIR="${THERM_INIT_DIR}"

        # A node name is required
        ! therm cluster enable --address="127.0.0.1:8090"

        # Enable clustering
        therm cluster enable --address="127.0.0.1:8090" node1
        therm cluster list --address="127.0.0.1:8090" | grep -q node1
        therm cluster show --address="127.0.0.1:8090" node1 | grep -q node1

        # Clustering can't be enabled on an already clustered instance.
        ! therm cluster enable --address="127.0.0.1:8090" node2
    )

    kill_therm "${TEST_DIR}" "${THERM_INIT_DIR}" 8090
}

test_clustering_membership() {
    THERM_ONE_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_ONE_DIR}"
    spawn_therm "${TEST_DIR}" "${THERM_ONE_DIR}" 8090
    echo "==> Spawn additional cluster node1 in ${THERM_ONE_DIR}"

    THERM_TWO_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_TWO_DIR}"
    spawn_therm "${TEST_DIR}" "${THERM_TWO_DIR}" 9000
    echo "==> Spawn additional cluster node2 in ${THERM_TWO_DIR}"

    local leader cert
    leader="127.0.0.1:8090"

    (
        set -e

        local address

        THERM_DIR="${THERM_ONE_DIR}"
        address="--address=$leader"

        therm cluster enable "${address}" node1
    )

    [ -f "${THERM_ONE_DIR}/server.crt" ]

    (
        set -e

        local address

        THERM_DIR="${THERM_TWO_DIR}"
        address="--address=127.0.0.1:9000"

        cat <<EOF | therm cluster join "${address}" node2 "${leader}" -
config:
  cluster-cert: |
$(cat ${THERM_ONE_DIR}/server.crt | sed -e 's/^/    /')
  cluster-key: |
$(cat ${THERM_ONE_DIR}/server.key | sed -e 's/^/    /')
EOF
    )

    # Configuration keys can be changed on any node.
    echo "==> Check cluster config"
    (
        set -e

        THERM_DIR="${THERM_ONE_DIR}" therm config set --address="127.0.0.1:8090" cluster.offline_threshold 30
        THERM_DIR="${THERM_ONE_DIR}" therm info --address="127.0.0.1:8090" | grep -q 'cluster.offline_threshold: "30"'
        THERM_DIR="${THERM_TWO_DIR}" therm info --address="127.0.0.1:9000" | grep -q 'cluster.offline_threshold: "30"'

        THERM_DIR="${THERM_TWO_DIR}" therm config set --address="127.0.0.1:9000" cluster.offline_threshold 40
        THERM_DIR="${THERM_ONE_DIR}" therm info --address="127.0.0.1:8090" | grep -q 'cluster.offline_threshold: "40"'
        THERM_DIR="${THERM_TWO_DIR}" therm info --address="127.0.0.1:9000" | grep -q 'cluster.offline_threshold: "40"'
    )

    echo "==> Spawn more nodes"

    THERM_THREE_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_THREE_DIR}"
    spawn_and_join_therm "${TEST_DIR}" "${THERM_THREE_DIR}" "node3" 9010 "${leader}" "${THERM_ONE_DIR}"

    THERM_FOUR_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_FOUR_DIR}"
    spawn_and_join_therm "${TEST_DIR}" "${THERM_FOUR_DIR}" "node4" 9020 "${leader}" "${THERM_ONE_DIR}"

    THERM_FIVE_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_FIVE_DIR}"
    spawn_and_join_therm "${TEST_DIR}" "${THERM_FIVE_DIR}" "node5" 9030 "${leader}" "${THERM_ONE_DIR}"

    echo "==> Check cluster list"
    (
        set -e

        THERM_DIR="${THERM_ONE_DIR}" therm cluster list --address="127.0.0.1:8090" --format="json" | jq '.[] | select(.server_name == "node1") | .database' | grep -q "true"
        THERM_DIR="${THERM_TWO_DIR}" therm cluster list --address="127.0.0.1:9000" --format="json" | jq '.[] | select(.server_name == "node2") | .database' | grep -q "true"
        THERM_DIR="${THERM_THREE_DIR}" therm cluster list --address="127.0.0.1:9010" --format="json" | jq '.[] | select(.server_name == "node3") | .database' | grep -q "true"
        THERM_DIR="${THERM_FOUR_DIR}" therm cluster list --address="127.0.0.1:9020" --format="json" | jq '.[] | select(.server_name == "node4") | .database' | grep -q "false"
        THERM_DIR="${THERM_FIVE_DIR}" therm cluster list --address="127.0.0.1:9030" --format="json" | jq '.[] | select(.server_name == "node5") | .database' | grep -q "false"
    )

    echo "==> Check single node"
    (
        set -e

        THERM_DIR="${THERM_TWO_DIR}" therm cluster show --address="127.0.0.1:9000" --format="json" node5 | jq '.server_name' | grep -q "node5"
    )

    echo "==> Check cluster failover"
    (
        set -e

        THERM_DIR="${THERM_ONE_DIR}" therm config set --address="127.0.0.1:8090" cluster.offline_threshold 5
        THERM_DIR="${THERM_TWO_DIR}" therm info --address="127.0.0.1:9000" | grep -q 'cluster.offline_threshold: "5"'
        THERM_DIR="${THERM_THREE_DIR}" therm daemon shutdown --address="127.0.0.1:9010"

        sleep 10

        THERM_DIR="${THERM_TWO_DIR}" therm cluster list --address="127.0.0.1:9000" --format="json" | jq '.[] | select(.server_name == "node3") | .status' | grep -q "offline"
        THERM_DIR="${THERM_TWO_DIR}" therm config set --address="127.0.0.1:9000" cluster.offline_threshold 30
        THERM_DIR="${THERM_ONE_DIR}" therm info --address="127.0.0.1:8090" | grep -q 'cluster.offline_threshold: "30"'
        THERM_DIR="${THERM_TWO_DIR}" therm cluster remove --address="127.0.0.1:9000" --force node3

        sleep 10

        THERM_DIR="${THERM_FOUR_DIR}" therm cluster list --address="127.0.0.1:9020" --format="json" | jq '.[] | select(.server_name == "node4") | .database' | grep -q "true"
    )

    kill_therm "${TEST_DIR}" "${THERM_ONE_DIR}" 8090
    kill_therm "${TEST_DIR}" "${THERM_TWO_DIR}" 9000
    kill_therm "${TEST_DIR}" "${THERM_THREE_DIR}" 9010
    kill_therm "${TEST_DIR}" "${THERM_FOUR_DIR}" 9020
    kill_therm "${TEST_DIR}" "${THERM_FIVE_DIR}" 9030
}
