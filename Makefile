REPO            ?= riak_mesos_scheduler
GIT_REF         ?= $(shell git describe --all)
GIT_TAG_VERSION ?= $(shell git describe --tags)
PKG_VERSION	    ?= $(shell git describe --tags --abbrev=0 | tr - .)
MAJOR           ?= $(shell echo $(PKG_VERSION) | cut -d'.' -f1)
MINOR           ?= $(shell echo $(PKG_VERSION) | cut -d'.' -f2)
ARCH            ?= amd64
OSNAME          ?= ubuntu
OSVERSION       ?= trusty
S3_BASE         ?= riak-tools
S3_PREFIX       ?= http://$(S3_BASE).s3.amazonaws.com/
DEPLOY_BASE     ?= $(REPO)/$(MAJOR).$(MINOR)/$(PKG_VERSION)/$(OSNAME)/$(OSVERSION)/
PKGNAME         ?= $(REPO)-$(PKG_VERSION)-$(ARCH).tar.gz

BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar
OVERLAY_VARS    ?=

ifneq (,$(shell whereis sha256sum | awk '{print $2}';))
SHASUM = sha256sum
else
SHASUM = shasum -a 256
endif

.PHONY: all compile recompile deps cleantest test rel clean relclean stage tarball

all: compile
compile: deps
	$(REBAR) compile
recompile:
	$(REBAR) compile skip_deps=true
clean: cleantest relclean
	$(REBAR) clean
	-rm -rf packages
clean-deps:
	$(REBAR) -r clean
rebar.config.lock:
	$(REBAR) get-deps compile
	$(REBAR) lock-deps
clean-lock:
	rm rebar.config.lock
lock: clean-lock distclean rebar.config.lock
deps: rebar.config.lock
	$(REBAR) -C rebar.config.lock get-deps
cleantest:
	rm -rf .eunit/*
	rm -rf ct_log/*
test: cleantest
	$(REBAR) skip_deps=true eunit
	$(REBAR) skip_deps=true ct
rel: relclean deps compile
	$(REBAR) compile
	$(REBAR) skip_deps=true generate $(OVERLAY_VARS)
relclean:
	-rm -rf rel/riak_mesos_scheduler
distclean: clean
	$(REBAR) delete-deps
stage: rel
	$(foreach dep,$(wildcard deps/*), rm -rf rel/riak_mesos_scheduler/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) rel/riak_mesos_scheduler/lib;)
	$(foreach app,$(wildcard apps/*), rm -rf rel/riak_mesos_scheduler/lib/$(shell basename $(app))-* && ln -sf $(abspath $(app)) rel/riak_mesos_scheduler/lib;)

##
## Packaging targets
##
tarball: rel
	echo "Creating packages/"$(PKGNAME)
	mkdir -p packages
	echo "$(GIT_REF)" > rel/version
	echo "$(GIT_TAG_VERSION)" >> rel/version
	tar -C rel -czf $(PKGNAME) version $(REPO)/
	rm rel/version
	mv $(PKGNAME) packages/
	cd packages && $(SHASUM) $(PKGNAME) > $(PKGNAME).sha
	cd packages && echo "$(S3_PREFIX)$(DEPLOY_BASE)$(PKGNAME)" > remote.txt
	cd packages && echo "$(BASE_DIR)/packages/$(PKGNAME)" > local.txt
sync:
	echo "Uploading to "$(DEPLOY_BASE)
	cd packages && \
		s3cmd put --acl-public $(PKGNAME) s3://$(S3_BASE)/$(DEPLOY_BASE) && \
		s3cmd put --acl-public $(PKGNAME).sha s3://$(S3_BASE)/$(DEPLOY_BASE)
