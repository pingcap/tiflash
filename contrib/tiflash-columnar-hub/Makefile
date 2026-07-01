SHELL := bash
ENABLE_FEATURES ?=
PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
UNAME_S := $(shell uname -s)

# Build-time environment, captured for reporting by the application binary.
BUILD_INFO_GIT_FALLBACK := "Unknown (no git or not git repo)"
BUILD_INFO_RUSTC_FALLBACK := "Unknown"
export PROXY_BUILD_TIME := ""
export PROXY_BUILD_RUSTC_VERSION := $(shell rustc --version 2> /dev/null || echo ${BUILD_INFO_RUSTC_FALLBACK})
export PROXY_BUILD_GIT_HASH ?= $(shell git -C $(PROJECT_DIR) rev-parse HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})
export PROXY_BUILD_GIT_BRANCH ?= $(shell git -C $(PROJECT_DIR) rev-parse --abbrev-ref HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})
export PROXY_PROFILE ?= debug
export PROMETHEUS_METRIC_NAME_PREFIX ?= tiflash_proxy_

# `kvengine` pulls in both `usearch` and the standalone `simsimd` crate from
# cloud-storage-engine. They currently export the same C symbols, so allow the
# linker to coalesce them in the columnar hub build instead of vendoring
# another patched `usearch` copy locally.
ifneq ($(UNAME_S),Darwin)
export RUSTFLAGS := $(RUSTFLAGS) -C link-arg=-Wl,--allow-multiple-definition
endif

FEATURE_ARGS :=
ifneq ($(strip $(ENABLE_FEATURES)),)
FEATURE_ARGS += --features $(ENABLE_FEATURES)
endif

.PHONY: debug release build clean

build:
	cargo build --locked --manifest-path hub-runtime/Cargo.toml $(PROFILE_FLAG) $(FEATURE_ARGS)

debug: export PROXY_PROFILE=debug
debug:
	cargo build --locked --manifest-path hub-runtime/Cargo.toml $(FEATURE_ARGS)

release: export PROXY_PROFILE=release
release:
	cargo build --locked --manifest-path hub-runtime/Cargo.toml --release $(FEATURE_ARGS)

clean:
	cargo clean --manifest-path hub-runtime/Cargo.toml
