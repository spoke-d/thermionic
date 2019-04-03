test_static_analysis() {
  (
    set -e

    cd ../

    ## Functions starting by empty line
    OUT=$(grep -Rrn --exclude-dir=vendor "^$" -B1 . | grep "func " | grep -v "}$" || true)
    if [ -n "${OUT}" ]; then
      echo "ERROR: Functions must not start with an empty line: \n${OUT}"
      false
    fi

    ## Mixed tabs/spaces in scripts
    OUT=$(grep -PRrn --exclude-dir=vendor '\t' . | grep '\.sh:' || true)
    if [ -n "${OUT}" ]; then
      echo "ERROR: mixed tabs and spaces in script: ${OUT}"
      false
    fi

    ## Trailing whitespace in scripts
    OUT=$(grep -Rrn --exclude-dir=vendor " $" . | grep '\.sh:' || true)
    if [ -n "${OUT}" ]; then
      echo "ERROR: trailing whitespace in script: ${OUT}"
      false
    fi

    ## go vet, if it exists
    if go help vet >/dev/null 2>&1; then
      go vet ./...
    fi

    ## golint
    if which golint >/dev/null 2>&1; then
      golint -set_exit_status ./
    fi

    ## deadcode
    if which deadcode >/dev/null 2>&1; then
      OUT=$(deadcode ./ 2>&1 || true)
      if [ -n "${OUT}" ]; then
        echo "${OUT}" >&2
        false
      fi
    fi

    ## misspell
    if which misspell >/dev/null 2>&1; then
      OUT=$(misspell 2>/dev/null ./ | grep -Ev "^vendor/" || true)
      if [ -n "${OUT}" ]; then
        echo "Found some typos"
        echo "${OUT}"
        exit 1
      fi
    fi

    ## ineffassign
    if which ineffassign >/dev/null 2>&1; then
      ineffassign ./
    fi

    # Skip the tests which require git
    if ! git status; then
      return
    fi

    # go fmt
    gofmt -w -s ./

    git add -u :/
    git diff --exit-code
  )
}