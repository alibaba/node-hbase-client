TESTS = test/*.test.js
REPORTER = spec
TIMEOUT = 10000
MOCHA_OPTS =

install:
	@npm install --registry=http://r.cnpmjs.org

test: install
	@NODE_ENV=test ./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		--timeout $(TIMEOUT) \
		$(MOCHA_OPTS) \
		$(TESTS)

test-cov: install
	@NODE_HBASE_CLENT_COV=1 $(MAKE) test MOCHA_OPTS="--require blanket" REPORTER=html-cov | ./node_modules/.bin/cov

test-all: test test-cov

test-coveralls:
	@$(MAKE) test
	@echo TRAVIS_JOB_ID $(TRAVIS_JOB_ID)
	@$(MAKE) test MOCHA_OPTS='--require blanket' REPORTER=mocha-lcov-reporter | ./node_modules/coveralls/bin/coveralls.js

contributors: install
	@./node_modules/.bin/contributors -f plain -o AUTHORS

autod: install
	@./node_modules/.bin/autod -w -e examples -k zookeeper-watcher

.PHONY: test
