# kvcouchbase-provider Makefile

CAPABILITY_ID = "wasmcloud:example:kvcouchbase_provider"
NAME = "kvcouchbase-provider"
VENDOR = "Couchbase"
PROJECT = kvcouchbase_provider
VERSION = 0.1.0
REVISION = 0

include ./provider.mk

test::
	cargo clippy --all-targets --all-features

