TESTS = test/*.test.js
REPORTER = spec
TIMEOUT = 15000
MOCHA_OPTS =

install:
	@npm install

jshint:
	@./node_modules/.bin/jshint .

test:
	@NODE_ENV=test ./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		--timeout $(TIMEOUT) \
		--bail \
		$(MOCHA_OPTS) \
		$(TESTS)

test-cov cov:
	@NODE_ENV=test node \
		node_modules/.bin/istanbul cover \
		node_modules/.bin/_mocha \
		-- -u exports \
		--reporter $(REPORTER) \
		--timeout $(TIMEOUT) \
		$(MOCHA_OPTS) \
		$(TESTS)

test-all: jshint test

test-travis:
	@NODE_ENV=test node \
		node_modules/.bin/istanbul cover \
		node_modules/.bin/_mocha --report lcovonly \
		-- --check-leaks \
		--reporter $(REPORTER) \
		--timeout $(TIMEOUT) \
		$(MOCHA_OPTS) \
		$(TESTS)

contributors:
	@./node_modules/.bin/contributors -f plain -o AUTHORS

autod:
	@./node_modules/.bin/autod -w -e examples -k zookeeper-watcher --prefix "~"

.PHONY: test
