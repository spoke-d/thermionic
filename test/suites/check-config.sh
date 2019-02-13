# Test the config command.
test_config() {
    # Invalid arguments
    ! therm config
    ! therm config foo
    ! therm config foo

    # Local database query
    therm config show | grep -q "core.https_address"
    therm config get "core.https_address" | grep -q ":8080"

    therm config set "core.debug_address" ":8082" | grep -q ":8082"

    # Cluster database query
    therm config set "core.proxy_http" "http://meshuggah.rocks" | grep -q "http://meshuggah.rocks"

    therm config show | grep -q "core.proxy_http"
    therm config get "core.proxy_http" | grep -q "http://meshuggah.rocks"
}