'use strict';

var should = require('should');

var buffers = {
  foo: Buffer.alloc(3, 'foo'),
  bar: Buffer.alloc(3, 'bar'),
  baz: Buffer.alloc(3, 'baz'),
  val: Buffer.alloc(3, 'val')
};

function testError(exp, msg) {
  try {
    exp();
  } catch(e) {
    e.message.should.equal(msg);
    return;
  }

  (true).should.equal('expected [' + msg + '] but non-expected dist');
}

function each(method) {
  var Clz = require('../lib/check_and_' + method);
  var Inner = require('../lib/' + method);

  var camel = method.split('');
  camel[0] = camel[0].toUpperCase();
  camel = camel.join('');

  describe('CheckAnd' + camel, function() {
    var inner = new Inner('key');
    it('should create CheckAnd' + camel + ' objects', function() {
      var obj = new Clz('foo', 'bar', 'baz', 'val', inner);

      obj.getRow().should.be.instanceof(Buffer);
      obj.getFamily().should.be.instanceof(Buffer);
      obj.getQualifier().should.be.instanceof(Buffer);
      obj.getValue().should.be.instanceof(Buffer);
      obj.getRow().toString().should.equal('foo');
      obj.getFamily().toString().should.equal('bar');
      obj.getQualifier().toString().should.equal('baz');
      obj.getValue().toString().should.equal('val');
      obj['get' + camel]().should.equal(inner);

      obj = new Clz(buffers.foo, buffers.bar, buffers.baz, buffers.val, inner);

      obj.getRow().should.be.instanceof(Buffer);
      obj.getFamily().should.be.instanceof(Buffer);
      obj.getQualifier().should.be.instanceof(Buffer);
      obj.getValue().should.be.instanceof(Buffer);
      obj.getRow().toString().should.equal('foo');
      obj.getFamily().toString().should.equal('bar');
      obj.getQualifier().toString().should.equal('baz');
      obj.getValue().toString().should.equal('val');
      obj['get' + camel]().should.equal(inner);

      var obj = new Clz('foo', 'bar', 'baz', null, inner);

      obj.getRow().should.be.instanceof(Buffer);
      obj.getFamily().should.be.instanceof(Buffer);
      obj.getQualifier().should.be.instanceof(Buffer);
      obj.getValue().should.be.instanceof(Buffer);
      obj.getRow().toString().should.equal('foo');
      obj.getFamily().toString().should.equal('bar');
      obj.getQualifier().toString().should.equal('baz');
      obj.getValue().toString().should.equal('');
      obj['get' + camel]().should.equal(inner);
    });

    it('should throw errors', function() {
      testError(() => new Clz(), 'Row key is invalid');
      testError(() => new Clz('foo'), 'Family is invalid');
      testError(() => new Clz('foo', 'bar'), 'Qualifier is invalid');
    });
  });
}

describe('test/check_and_foo.test.js', function() {
  [ 'put', 'delete' ].forEach(each);
});
