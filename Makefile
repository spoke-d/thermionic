PATH_THERMIONIC = github.com/spoke-d/thermionic
VERSION=$(shell grep "const Version" internal/version/version.go | cut -d'"' -f2)
TAG_SQLITE3=$(shell printf "\#include <sqlite3.h>\nvoid main(){int n = SQLITE_IOERR_NOT_LEADER;}" | $(CC) ${CGO_CFLAGS} -o /dev/null -xc - >/dev/null 2>&1 && echo "libsqlite3")

.PHONY: default
default:
ifeq ($(TAG_SQLITE3),)
	@echo "Missing custom libsqlite3, run \"make deps\" to setup."
	exit 1
endif

	$(MAKE) setup
	@echo "Built successfully"

setup:
	go get -u github.com/golang/dep/cmd/dep
	go get -u github.com/golang/mock/mockgen
	dep ensure -vendor-only

build: bin/therm

install:
	go install -v -tags "$(TAG_SQLITE3)" $(PATH_THERMIONIC)/cmd/therm/...

clean:
	rm -f bin/therm

bin/therm:
	go build -o bin/therm ${PATH_THERMIONIC}/cmd/therm

.PHONY: check
check: clean build install
	go test -v -tags "$(TAG_SQLITE3) integration" ./...
	cd test && ./main.sh

.PHONY: update
update:
	go get -t -v -d -u ./...
	@echo "Dependencies updated"

.PHONY: deps
deps:
	# sqlite
	@if [ -d "$(GOPATH)/deps/sqlite" ]; then \
		cd "$(GOPATH)/deps/sqlite"; \
		git pull; \
	else \
		git clone --depth=1 "https://github.com/CanonicalLtd/sqlite" "$(GOPATH)/deps/sqlite"; \
	fi

	cd "$(GOPATH)/deps/sqlite" && \
		./configure --enable-replication --disable-amalgamation --disable-tcl && \
		git log -1 --format="format:%ci%n" | sed -e 's/ [-+].*$$//;s/ /T/;s/^/D /' > manifest && \
		git log -1 --format="format:%H" > manifest.uuid && \
		make

	# dqlite
	@if [ -d "$(GOPATH)/deps/dqlite" ]; then \
		cd "$(GOPATH)/deps/dqlite"; \
		git pull; \
	else \
		git clone --depth=1 "https://github.com/CanonicalLtd/dqlite" "$(GOPATH)/deps/dqlite"; \
	fi

	cd "$(GOPATH)/deps/dqlite" && \
		autoreconf -i && \
		PKG_CONFIG_PATH="$(GOPATH)/deps/sqlite/" ./configure && \
		make CFLAGS="-I$(GOPATH)/deps/sqlite/"

	# environment
	@echo ""
	@echo "Please set the following in your environment (possibly ~/.bashrc)"
	@echo "export CGO_CFLAGS=\"-I$(GOPATH)/deps/sqlite/ -I$(GOPATH)/deps/dqlite/include/\""
	@echo "export CGO_LDFLAGS=\"-L$(GOPATH)/deps/sqlite/.libs/ -L$(GOPATH)/deps/dqlite/.libs/\""
	@echo "export LD_LIBRARY_PATH=\"$(GOPATH)/deps/sqlite/.libs/:$(GOPATH)/deps/dqlite/.libs/\""

generate:
	go generate -v ./...
	# find . -type f -name '*.go' | grep -v '^./vendor' | xargs dirname | sort | uniq | xargs go generate -v
