REBAR ?= $(shell which rebar3 2>/dev/null)

TEST_DIR=$(CURDIR)/test


TEST_MODULES="db,db_range,iterators,batch,snapshot,column_family,batch,cache,blob_db,checkpoint,db_backup,cleanup,fifo_compaction,in_mem,merge,rate_limiter,sst_file_manager,transaction,transaction_log,ttl,write_buffer_manager,statistics,get_property"

TEST_ALL_MODULES="${TEST_MODULES},compression"

all: compile test

compile:
	@${REBAR} compile

test: compile
	@${REBAR} eunit --module=${TEST_MODULES}

test_all: compile
	@${REBAR} eunit --module=${TEST_ALL_MODULES}

clean:
	@${REBAR} clean

distclean: clean
	@git clean -fdx
