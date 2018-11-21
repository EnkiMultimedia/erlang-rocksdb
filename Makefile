REBAR ?= ./deps/rebar3

all: compile test

compile:
	@${REBAR} compile

test: compile
	@${REBAR} eunit

clean:
	@${REBAR} clean
