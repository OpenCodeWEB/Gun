/**
 * test/yjs/yjs.js — tests for gun/lib/yjs.js, the GunDB provider for Yjs.
 *
 * Run with:  npm run testyjs   (mocha test/yjs/yjs.js)
 *
 * Requires the optional dev dependencies: yjs and y-protocols.
 * (c) ABsUP / OpenCodeWEB. MIT License.
 */
var root = typeof global !== 'undefined' ? global : window;
(function(){
  try{ require('fs').unlinkSync('data.json') }catch(e){}
  try{ require('../../lib/fsrm')('radatatest') }catch(e){}
  root.Gun = require('../../gun');
  // NOTE: Gun.TESTING is intentionally NOT set — lib/store.js would force
  // every gun instance onto the same 'radatatest' store, which breaks the
  // cross-peer relay test. Tests pass explicit per-instance `file` options.
  require('../../lib/store');
  require('../../lib/rfs');
  try{ root.expect = require('../expect') }catch(e){}
}(this));

;(function(){
var Gun = root.Gun,
    expect = root.expect,
    Y = require('yjs'),
    GunYjsProvider = require('../../lib/yjs'),
    awarenessProtocol = null;
try{ awarenessProtocol = require('y-protocols/awareness') }catch(e){}

if(!Gun || !Y){ return }

describe('GunYjsProvider', function(){
  this.timeout(1000 * 30);

  function cleanup(){
    // Let pending fire-and-forget puts settle before removing stores.
    return wait(250).then(function(){
      try{ require('../../lib/fsrm')('radatatest') }catch(e){}
      try{ require('../../lib/fsrm')('radata-yjs-a') }catch(e){}
      try{ require('../../lib/fsrm')('radata-yjs-b') }catch(e){}
    });
  }

  afterEach(cleanup);

  function wait(ms){ return new Promise(function(r){ setTimeout(r, ms) }) }

  function onceSynced(provider){
    return new Promise(function(resolve){
      if(provider.synced){ return resolve(true) }
      provider.on('sync', function(s){ if(s){ resolve(true) } });
    });
  }

  function until(fn, tries){
    tries = tries || 50;
    return new Promise(function(resolve){
      (function check(){
        var out;
        try{ out = fn() }catch(e){ out = false }
        if(out){ return resolve(true) }
        if(--tries <= 0){ return resolve(false) }
        setTimeout(check, 100);
      }());
    });
  }

  it('syncs a Y.Text edit between two providers on one gun', async function(){
    var gun = Gun({file: 'radatatest'});
    var d1 = new Y.Doc(), d2 = new Y.Doc();
    var p1 = new GunYjsProvider(gun, 'room1', d1);
    var p2 = new GunYjsProvider(gun, 'room1', d2);
    await Promise.all([onceSynced(p1), onceSynced(p2)]);
    d1.getText('t').insert(0, 'hello');
    var ok = await until(function(){
      return d2.getText('t').toString() === 'hello';
    });
    expect(ok).to.be(true);
    p1.destroy(); p2.destroy();
  });

  it('converges concurrent edits from both clients', async function(){
    var gun = Gun({file: 'radatatest'});
    var d1 = new Y.Doc(), d2 = new Y.Doc();
    var p1 = new GunYjsProvider(gun, 'room2', d1);
    var p2 = new GunYjsProvider(gun, 'room2', d2);
    await Promise.all([onceSynced(p1), onceSynced(p2)]);
    d1.getText('t').insert(0, 'A');
    d2.getText('t').insert(0, 'B');
    var ok = await until(function(){
      var s1 = d1.getText('t').toString(), s2 = d2.getText('t').toString();
      return s1.length === 2 && s2.length === 2 &&
        s1.indexOf('A') > -1 && s1.indexOf('B') > -1 && s1 === s2;
    });
    expect(ok).to.be(true);
    p1.destroy(); p2.destroy();
  });

  it('loads existing content when joining a room late', async function(){
    var gun = Gun({file: 'radatatest'});
    var d1 = new Y.Doc();
    var p1 = new GunYjsProvider(gun, 'room3', d1);
    await onceSynced(p1);
    d1.getText('t').insert(0, 'existing');
    await wait(500); // let the cumulative state persist.
    var d2 = new Y.Doc();
    var p2 = new GunYjsProvider(gun, 'room3', d2);
    await onceSynced(p2);
    expect(d2.getText('t').toString()).to.be('existing');
    p1.destroy(); p2.destroy();
  });

  it('offline edits converge after reconnect', async function(){
    var gun = Gun({file: 'radatatest'});
    var d1 = new Y.Doc(), d2 = new Y.Doc();
    var p1 = new GunYjsProvider(gun, 'room4', d1);
    var p2 = new GunYjsProvider(gun, 'room4', d2);
    await Promise.all([onceSynced(p1), onceSynced(p2)]);
    p2.disconnect();
    d1.getText('t').insert(0, 'online');
    d2.getText('t').insert(0, 'offline');
    await wait(300);
    expect(d2.getText('t').toString()).to.be('offline'); // local edits work offline.
    p2.connect();
    var ok = await until(function(){
      var s1 = d1.getText('t').toString(), s2 = d2.getText('t').toString();
      return s1.length === 13 && s1 === s2 &&
        s1.indexOf('online') > -1 && s1.indexOf('offline') > -1;
    });
    expect(ok).to.be(true);
    p1.destroy(); p2.destroy();
  });

  it('syncs across two gun peers over a relay', async function(done){
    var http = require('http');
    var server = http.createServer(function(req, res){
      res.writeHead(404); res.end();
    });
    server.on('error', function(e){ done(e) });
    server.listen(0, function(){
      var port = server.address().port;
      var gunA = Gun({file: 'radata-yjs-a', web: server, localStorage: false, axe: false});
      var gunB = Gun({file: 'radata-yjs-b', peers: ['ws://127.0.0.1:'+port+'/gun'], localStorage: false, axe: false});
      var d1 = new Y.Doc(), d2 = new Y.Doc();
      var p1 = new GunYjsProvider(gunA, 'room5', d1);
      var p2 = new GunYjsProvider(gunB, 'room5', d2);
      // Debug aid: confirm the peer link is established.
      gunB.get('yjs').get('room5').get('c').once(function(node){
        console.log('DEBUG B sees c node keys:', node ? Object.keys(node) : null);
      });
      p2.on('sync', async function(synced){
        if(!synced){ return }
        d1.getText('t').insert(0, 'across');
        var ok = await until(function(){
          return d2.getText('t').toString() === 'across';
        }, 100);
        console.log('DEBUG converged across relay:', ok, '| d1:', d1.getText('t').toString(), '| d2:', d2.getText('t').toString());
        expect(ok).to.be(true);
        p1.destroy(); p2.destroy();
        try{ server.close() }catch(e){}
        done();
      });
    });
  });

  it('shares awareness states between clients', async function(){
    if(!awarenessProtocol){ return this.skip() }
    var gun = Gun({file: 'radatatest'});
    var d1 = new Y.Doc(), d2 = new Y.Doc();
    var a1 = new awarenessProtocol.Awareness(d1);
    var a2 = new awarenessProtocol.Awareness(d2);
    var p1 = new GunYjsProvider(gun, 'room6', d1, {awareness: a1});
    var p2 = new GunYjsProvider(gun, 'room6', d2, {awareness: a2});
    await Promise.all([onceSynced(p1), onceSynced(p2)]);
    a1.setLocalState({user: 'alice'});
    var ok = await until(function(){
      var state = a2.getStates().get(p1.clientId);
      return state && state.user === 'alice';
    });
    expect(ok).to.be(true);
    p1.destroy(); p2.destroy();
  });
});

}());