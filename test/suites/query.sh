# Test the query command.
test_query() {
    # Invalid arguments
    ! therm query
    ! therm query foo
    ! therm query foo "SELECT * FROM CONFIG"
    ! therm query global ""

    # Local database query
    therm query local '{"queries":["SELECT * FROM config"]}' | grep -q "core.https_address"

    therm query global '{"queries":["INSERT INTO config(key,value) VALUES(\"foo\",\"bar\")"]}' | grep -q "Rows affected: 1"
    therm query global '{"queries":["DELETE FROM config WHERE key=\"foo\""]}' | grep -q "Rows affected: 1"

    # Multiple queries
    therm query global '{"queries":["SELECT * FROM config; SELECT * FROM nodes"]}' | grep -q "=> Query 0"

    # Overloaded query
    cat <<EOF | therm query local -
queries:
  - SELECT * FROM config
EOF
}