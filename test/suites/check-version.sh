test_check_version() {
    thermionic_version=$(therm version | grep "client_version: " | cut -d' ' -f2)
    thermionic_major=$(echo "${thermionic_version}" | xargs printf "%s" | cut -d. -f1)
    [ "${thermionic_major}" = "0" ]
}