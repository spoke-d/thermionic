test_schedule() {
    # Invalid arguments
    ! therm schedule

    local now ident

    now=$(date -u --rfc-3339=seconds --date="+ 30 seconds" | sed 's/ /T/')

    therm schedule add "${now}" "{'queries': ['SELECT * FROM tasks']}"
    therm schedule list --format=json | jq '.[] | .status' | grep -q "pending"

    echo "==> Waiting for schedule to trigger"
    sleep 40

    therm schedule list --format=json | jq '.[] | .status' | grep -q "success"
    ident=$(therm schedule list --format=json | jq '.[] | .id' | sed 's/^"\(.*\)"$/\1/')
    therm schedule show --format=json "${ident}" | jq '.status' | grep -q "success"

    therm schedule delete "${ident}"
}