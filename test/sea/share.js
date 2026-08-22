var root;
var Gun;
(function(){
  var env;
  if(typeof global !== 'undefined'){ env = global }
  if(typeof window !== 'undefined'){ env = window }
  root = env.window? env.window : global;
  try{ env.window && root.localStorage && root.localStorage.clear() }catch(e){}
  if(root.Gun){
    root.Gun = root.Gun;
    root.Gun.TESTING = true;
  } else {
    try{ require('fs').unlinkSync('data.json') }catch(e){}
    try{ require('../../lib/fsrm')('radatatest') }catch(e){}
    root.Gun = require('../../gun');
    root.Gun.TESTING = true;
    require('../../lib/store');
    require('../../lib/rfs');
  }

  try{ var expect = global.expect = require("../expect") }catch(e){}

  if(!root.Gun.SEA){
    require('../../sea.js');
  }
}(this));

;(function(){
Gun = root.Gun
var SEA = Gun.SEA
if(!SEA){ return }

describe('SEA.share', function(){
  this.timeout(1000 * 9);
  var alice, bob, carol;

  before(async function(){
    alice = await SEA.pair();
    bob = await SEA.pair();
    carol = await SEA.pair();
  });

  it('round-trips a string from Alice to Bob (zero-knowledge box)', async function(){
    var box = await SEA.share('hello bob, from alice', bob.pub, alice);
    expect(box).to.be.an('object');
    expect(box.box).to.be.a('string');
    expect(box.s).to.be(alice.pub);
    expect(box.k).to.be.an('object');
    expect(box.k[bob.pub]).to.be.a('string');
    // zero-knowledge: plaintext must never appear in the box
    expect(box.box.indexOf('hello bob')).to.be(-1);
    var dec = await SEA.open(box, alice.pub, bob);
    expect(dec).to.be('hello bob, from alice');
  });

  it('round-trips an object (JSON serialization)', async function(){
    var data = { msg: 'secret plan', n: 42, nested: { ok: true } };
    var box = await SEA.share(data, [bob.pub], alice);
    var dec = await SEA.open(box, alice.pub, bob);
    expect(dec).to.eql(data);
  });

  it('supports multiple recipients; each can open, sender can open', async function(){
    var data = 'multi-recipient secret';
    var box = await SEA.share(data, [bob.pub, carol.pub], alice);
    expect(Object.keys(box.k).sort()).to.eql([bob.pub, carol.pub].sort());
    expect(await SEA.open(box, alice.pub, bob)).to.be(data);
    expect(await SEA.open(box, alice.pub, carol)).to.be(data);
    // sender can also open (their own pub is in k if they include it)
    var box2 = await SEA.share(data, [alice.pub, bob.pub], alice);
    expect(await SEA.open(box2, alice.pub, alice)).to.be(data);
  });

  it('accepts pair objects as recipients', async function(){
    var box = await SEA.share('by pair', bob, alice);
    expect(box.k[bob.pub]).to.be.a('string');
    expect(await SEA.open(box, alice.pub, bob)).to.be('by pair');
  });

  it('rejects a wrong recipient (cannot decrypt)', async function(){
    var box = await SEA.share('for bob only', bob.pub, alice);
    var evil = await SEA.pair();
    var dec = await SEA.open(box, alice.pub, evil);
    expect(dec).to.be(undefined); // AES-GCM auth failure -> SEA.err, no data
  });

  it('rejects a tampered box', async function(){
    var box = await SEA.share('do not touch', bob.pub, alice);
    box.box = box.box.slice(0, -4) + 'AAAA'; // corrupt ciphertext
    var dec = await SEA.open(box, alice.pub, bob);
    expect(dec).to.be(undefined);
  });

  it('works with callback style (parity with promise)', async function(){
    var done2 = this.async();
    SEA.share('cb style', bob.pub, alice, function(box){
      SEA.open(box, alice.pub, bob, function(dec){
        try{
          expect(dec).to.be('cb style');
          done2();
        }catch(e){ done2(e) }
      });
    });
  });

  it('throws early on missing recipients / invalid sender', async function(){
    var err1 = false;
    try{ await SEA.share('x', null, alice) }catch(e){ err1 = true }
    expect(err1).to.be(true);
    var err2 = false;
    try{ await SEA.share('x', bob.pub, { pub: alice.pub, priv: alice.priv }) }catch(e){ err2 = true }
    expect(err2).to.be(true); // no epub/epriv -> cannot derive
  });
});
}());
