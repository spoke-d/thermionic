spawn_therm() {
    set +x

    local THERM_DIR testdir thermdir

    testdir=${1}
    shift

    thermdir=${1}
    shift

    port=${1}
    shift

    networkport="${port}"
    debugport=$((port+1))

    echo "==> Spawning therm in ${thermdir}"

    THERM_DIR="${thermdir}" therm daemon init --network-port="${networkport}" --debug-port="${debugport}" "$@" 2>&1 &
    THERM_PID=$!
    echo "${THERM_PID}" > "${thermdir}/therm.pid"
    echo "${thermdir}" >> "${testdir}/daemons"

    echo "==> Confirming therm is responsive"
    THERM_DIR="${thermdir}" therm daemon waitready --address="127.0.0.1:${networkport}" --timeout=300s

    sleep 1
    echo "==> Spawned therm (PID is ${THERM_PID})"
}

spawn_and_join_therm() {
    local testdir thermdir name port leader leaderdir

    testdir=${1}
    thermdir=${2}
    name=${3}
    port=${4}
    leader=${5}
    leaderdir=${6}

    spawn_therm "${testdir}" "${thermdir}" "${port}"
    echo "==> Spawn additional cluster ${name} in ${thermdir}"

    (
        set -e

        local address

        THERM_DIR="${thermdir}"
        address="--address=127.0.0.1:${port}"

        cat <<EOF | therm cluster join "${address}" "${name}" "${leader}" -
config:
  cluster-cert: |
$(cat ${leaderdir}/server.crt | sed -e 's/^/    /')
  cluster-key: |
$(cat ${leaderdir}/server.key | sed -e 's/^/    /')
EOF
    )

    echo "==> Spawned therm ${name}"
}

kill_therm() {
    local THERM_DIR testdir thermdir thermpid

    testdir=${1}
    thermdir=${2}
    port=${3}
    THERM_DIR=${thermdir}

    if [ ! -f "${thermdir}/therm.pid" ]; then
      return
    fi

    thermpid=$(cat "${thermdir}/therm.pid")

    echo "==> Killing therm at ${thermdir}"

    THERM_DIR="${thermdir}" therm daemon shutdown --address="127.0.0.1:${port}" || kill -9 "${thermpid}" 2>/dev/null || true

    sed "\\|^${thermdir}|d" -i "${testdir}/daemons"

    echo "==> Killed therm (PID is ${THERM_PID})"
}

cleanup_therms() {
    local testdir thermdir

    testdir=${1}

    # Kill all instances
    while read -r thermdir; do
        kill_therm "${testdir}" "${thermdir}" 8080
    done < "${testdir}/daemons"
}

spawn_therm_discovery() {
    set +x

    local THERM_DIR testdir thermdir

    testdir=${1}
    shift

    thermdir=${1}
    shift

    network_addr=${1}
    shift

    bind_addr=${1}
    shift

    leader_port=${1}
    shift

    leader_nonce=${1}
    shift

    subord_port=${1}
    shift

    subord_nonce=${1}
    shift

    debugport=$((network_addr+1))

    echo "==> Spawning therm discovery in ${thermdir}"

    THERM_DIR="${thermdir}" therm discover init \
        --network-port="${network_addr}" \
        --debug-port="${debugport}" \
        --bind-address="127.0.0.1:${bind_addr}" \
        "127.0.0.1:${leader_port}" \
        "${leader_nonce}" \
        "127.0.0.1:${subord_port}" \
        "${subord_nonce}" \
        "$@" 2>&1 &
    THERM_PID=$!
    echo "${THERM_PID}" > "${thermdir}/therm.pid"
    echo "${thermdir}" >> "${testdir}/discovery"

    echo "==> Confirming therm is responsive"
    THERM_DIR="${thermdir}" therm discover waitready --address="127.0.0.1:${networkport}" --timeout=300s

    sleep 1
    echo "==> Spawned therm discovery (PID is ${THERM_PID})"
}

kill_therm_discovery() {
    local THERM_DIR testdir thermdir thermpid

    testdir=${1}
    thermdir=${2}
    port=${3}
    THERM_DIR=${thermdir}

    if [ ! -f "${thermdir}/therm.pid" ]; then
      return
    fi

    thermpid=$(cat "${thermdir}/therm.pid")

    echo "==> Killing therm discovery at ${thermdir}"

    THERM_DIR="${thermdir}" therm discover shutdown --address="127.0.0.1:${port}" || kill -9 "${thermpid}" 2>/dev/null || true

    sed "\\|^${thermdir}|d" -i "${testdir}/discovery"

    echo "==> Killed therm discovery (PID is ${THERM_PID})"
}

cleanup_therms_discovery() {
    local testdir thermdir

    testdir=${1}

    # Kill all instances
    while read -r thermdir; do
        kill_therm_discovery "${testdir}" "${thermdir}" 8080
    done < "${testdir}/daemons"
}
