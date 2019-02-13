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