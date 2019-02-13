test_check_tests() {
    (
        set -e

        cd ../
        go test -v ./...
    )
}