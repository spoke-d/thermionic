test_discovery() {
    THERM_ONE_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_ONE_DIR}"
    spawn_therm "${TEST_DIR}" "${THERM_ONE_DIR}" 8090 "true"
    echo "==> Spawn additional cluster node1 in ${THERM_ONE_DIR}"

    THERM_TWO_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_TWO_DIR}"
    spawn_therm "${TEST_DIR}" "${THERM_TWO_DIR}" 9000 "true"
    echo "==> Spawn additional cluster node2 in ${THERM_TWO_DIR}"

    (
        set -e

        local pid

        THERM_DIR="${THERM_ONE_DIR}"

        ! therm discover init

        local LEADER_NONCE=$(head -n 1 "${THERM_ONE_DIR}/nonce.txt")
        local SUBORD_NONCE=$(head -n 1 "${THERM_TWO_DIR}/nonce.txt")

        # Enable clustering
        therm cluster enable --address="127.0.0.1:8090" node1

        THERM_DIR="${THERM_TWO_DIR}" therm discover init \
            --network-port="9082" \
            --debug-port="9083" \
            --bind-address="127.0.0.1:9085" \
            "127.0.0.1:8090" \
            "${LEADER_NONCE}" \
            "127.0.0.1:9000" \
            "${SUBORD_NONCE}" \
            2>&1 &
        pid=$!

        echo "==> Confirming therm discovery is responsive"
        THERM_DIR="${THERM_TWO_DIR}" therm discover waitready --address="127.0.0.1:9082" --timeout=300s
        echo "==> Spawned therm discovery (PID is ${pid})"
    )

    kill_therm "${TEST_DIR}" "${THERM_INIT_DIR}" 8090
}

test_discovery_join() {
    THERM_ONE_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_ONE_DIR}"
    spawn_therm "${TEST_DIR}" "${THERM_ONE_DIR}" 8090 "true"
    echo "==> Spawn additional cluster node1 in ${THERM_ONE_DIR}"

    THERM_TWO_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_TWO_DIR}"
    spawn_therm "${TEST_DIR}" "${THERM_TWO_DIR}" 9000 "true"
    echo "==> Spawn additional cluster node2 in ${THERM_TWO_DIR}"

    local leader="127.0.0.1:8090"

    (
        set -e

        local address

        THERM_DIR="${THERM_ONE_DIR}"
        address="--address=$leader"

        therm cluster enable "${address}" node1
    )
    
    # Setup discovery node for node2
    echo "==> Setup discovery node"
    local LEADER_NONCE=$(head -n 1 "${THERM_ONE_DIR}/nonce.txt")
    local SUBORD_NONCE=$(head -n 1 "${THERM_TWO_DIR}/nonce.txt")
    spawn_therm_discovery "${TEST_DIR}" "${THERM_TWO_DIR}" 9082 9085 8090 "${LEADER_NONCE}" 9000 "${SUBORD_NONCE}"

    sleep 10
    echo "==> Check cluster list"
     (
        set -e

        THERM_DIR="${THERM_ONE_DIR}" therm cluster list --address="127.0.0.1:8090" --format="json" | jq '.[] | select(.url == "127.0.0.1:8090") | .database' | grep -q "true"
        THERM_DIR="${THERM_TWO_DIR}" therm cluster list --address="127.0.0.1:9000" --format="json" | jq '.[] | select(.url == "127.0.0.1:9000") | .database' | grep -q "true"
    )

    kill_therm "${TEST_DIR}" "${THERM_ONE_DIR}" 8090
    kill_therm "${TEST_DIR}" "${THERM_TWO_DIR}" 9000
    kill_therm_discovery "${TEST_DIR}" "${THERM_TWO_DIR}" 9082
}


test_discovery_leave() {
    THERM_ONE_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_ONE_DIR}"
    spawn_therm "${TEST_DIR}" "${THERM_ONE_DIR}" 8090 "true"
    echo "==> Spawn additional cluster node1 in ${THERM_ONE_DIR}"

    THERM_TWO_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
    chmod +x "${THERM_TWO_DIR}"
    spawn_therm "${TEST_DIR}" "${THERM_TWO_DIR}" 9000 "true"
    echo "==> Spawn additional cluster node2 in ${THERM_TWO_DIR}"

    local leader="127.0.0.1:8090"

    (
        set -e

        local address

        THERM_DIR="${THERM_ONE_DIR}"
        address="--address=$leader"

        therm cluster enable "${address}" node1
    )
    
    # Setup discovery node for node2
    echo "==> Setup discovery node"
    local LEADER_NONCE=$(head -n 1 "${THERM_ONE_DIR}/nonce.txt")
    local SUBORD_NONCE=$(head -n 1 "${THERM_TWO_DIR}/nonce.txt")
    spawn_therm_discovery "${TEST_DIR}" "${THERM_TWO_DIR}" 9082 9085 8090 "${LEADER_NONCE}" 9000 "${SUBORD_NONCE}"

    sleep 10
    echo "==> Check cluster list"
    (
        set -e

        THERM_DIR="${THERM_ONE_DIR}" therm cluster list --address="127.0.0.1:8090" --format="json" | jq '.[] | select(.url == "127.0.0.1:8090") | .database' | grep -q "true"
        THERM_DIR="${THERM_TWO_DIR}" therm cluster list --address="127.0.0.1:9000" --format="json" | jq '.[] | select(.url == "127.0.0.1:9000") | .database' | grep -q "true"
    )

    kill_therm_discovery "${TEST_DIR}" "${THERM_TWO_DIR}" 9082

    sleep 10
    echo "==> Check cluster list"
    (
        set -e

        THERM_DIR="${THERM_ONE_DIR}" therm cluster list --address="127.0.0.1:8090" --format="json" | jq '.[] | select(.url == "127.0.0.1:8090") | .database' | grep -q "true"
        THERM_DIR="${THERM_ONE_DIR}" therm cluster list --address="127.0.0.1:8090" --format="json" | jq '.[] | select(.url == "127.0.0.1:9000")' | grep -q ""
    )


    kill_therm "${TEST_DIR}" "${THERM_ONE_DIR}" 8090
    kill_therm "${TEST_DIR}" "${THERM_TWO_DIR}" 9000
}
