test_check_deps() {
  ldd "$(which therm)" | grep -q -E "libsqlite3|libdqlite"
}