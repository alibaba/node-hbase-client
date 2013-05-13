TESTS = test/*.test.js
REPORTER = spec
TIMEOUT = 10000
MOCHA_OPTS = 

install:
	@npm install

test: install
	@NODE_ENV=test ./node_modules/mocha/bin/mocha \
		--reporter $(REPORTER) \
		--timeout $(TIMEOUT) \
		$(MOCHA_OPTS) \
		$(TESTS)

test-cov: install
	@NODE_HBASE_CLENT_COV=1 $(MAKE) test MOCHA_OPTS="--require blanket" REPORTER=dot
	@NODE_HBASE_CLENT_COV=1 $(MAKE) test MOCHA_OPTS="--require blanket" REPORTER=html-cov > coverage.html 

.PHONY: install test test-cov
