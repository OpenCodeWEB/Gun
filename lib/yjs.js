/**
 * gun/lib/yjs.js — GunDB transport + storage provider for Yjs CRDT documents.
 *
 * A self-contained Yjs provider: a Gun graph acts as BOTH the network
 * transport (peers who share a Gun relay exchange Yjs updates through a
 * common graph) and the durable store (updates persist in Gun's radisk, so a
 * client joining a room loads existing content even if no one is online).
 *
 * Sync model (v1):
 *   Every client owns one node:  yjs/<room>/c/<clientId> = { u, sv }
 *   where `u` is base64(Y.encodeStateAsUpdate(doc)) — the client's cumulative
 *   state — and `sv` the base64 state vector. Cumulative updates are
 *   monotonic (each contains every earlier one), so Gun's per-key
 *   last-write-wins resolution is safe, and the union of all clients' latest
 *   states always contains every change ever made. Clients watch
 *   yjs/<room>/c with .map().on() and apply incoming updates, which is
 *   idempotent in Yjs. Awareness states live under yjs/<room>/a/<clientId>
 *   (y-protocols encoding) and are refreshed periodically so Gun's LWW
 *   timestamps stay fresh.
 *
 * This module is OPTIONAL: `yjs` (and `y-protocols` for awareness) are
 * required lazily, keeping Gun core dependency-free.
 *
 * Usage:
 *   const Gun = require('gun');
 *   require('gun/lib/yjs');                       // registers Gun.chain.yjs
 *   const Y = require('yjs');
 *   const { Awareness } = require('y-protocols/awareness');
 *
 *   const gun = Gun();
 *   const doc = new Y.Doc();
 *   const provider = gun.yjs('room-name', doc, { awareness: new Awareness(doc) });
 *
 *   provider.on('sync', synced => { /* initial sync complete *\/ });
 *   provider.on('status', ({status}) => { /* connecting | connected | disconnected *\/ });
 *   ...
 *   provider.destroy();
 *
 * (c) ABsUP / OpenCodeWEB. MIT License. Contributed to the GUN ecosystem.
 */
(function(){ 'use strict';

var Gun = require('../gun'),
    u;

/* ---------- base64 (browser + node) ---------- */
var b64 = {
  encode: function(bytes){
    if(typeof Buffer !== u){ return Buffer.from(bytes).toString('base64') }
    var s = '', i = 0;
    for(; i < bytes.length; i++){ s += String.fromCharCode(bytes[i]) }
    return btoa(s);
  },
  decode: function(str){
    if(typeof Buffer !== u){ return new Uint8Array(Buffer.from(str, 'base64')) }
    var s = atob(str), bytes = new Uint8Array(s.length), i = 0;
    for(; i < s.length; i++){ bytes[i] = s.charCodeAt(i) }
    return bytes;
  }
};

/* ---------- tiny observable (y-websocket-compatible surface) ---------- */
function Observable(){
  this._events = {};
}
Observable.prototype.on = function(name, fn){
  (this._events[name] = this._events[name] || []).push(fn);
  return this;
};
Observable.prototype.off = function(name, fn){
  var list = this._events[name];
  if(!list){ return this }
  this._events[name] = list.filter(function(f){ return f !== fn });
  return this;
};
Observable.prototype.emit = function(name, data){
  var list = this._events[name], i = 0;
  if(!list){ return }
  for(; i < list.length; i++){ list[i](data) }
};

/* ---------- lazy optional dependencies ---------- */
function yjs(){
  var Y;
  try{ Y = require('yjs') }
  catch(e){
    throw 'gun/lib/yjs.js: the optional `yjs` package is required — run: npm install yjs';
  }
  return Y;
}
function awarenessProtocol(){
  var ap;
  try{ ap = require('y-protocols/awareness') }
  catch(e){
    throw 'gun/lib/yjs.js: awareness requires the optional `y-protocols` package — run: npm install y-protocols';
  }
  return ap;
}

/* ---------- client id ---------- */
function uid(){
  if(typeof crypto !== u && crypto.randomUUID){ return crypto.randomUUID() }
  return 'y' + Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
}

/* ---------- the provider ---------- */
function GunYjsProvider(gun, name, doc, opt){
  if(!(this instanceof GunYjsProvider)){ return new GunYjsProvider(gun, name, doc, opt) }
  opt = opt || {};
  if(!gun || typeof gun.get !== 'function'){
    throw 'gun/lib/yjs.js: a Gun instance is required (new GunYjsProvider(gun, name, doc, opt)).'
  }
  if(!doc || typeof doc.on !== 'function' || typeof doc.transact !== 'function'){
    throw 'gun/lib/yjs.js: a Y.Doc instance is required (new GunYjsProvider(gun, name, doc, opt)).'
  }
  Observable.call(this);
  this.gun = gun;
  this.name = String(name);
  this.doc = doc;
  this.opt = opt;
  this.clientId = opt.clientId || uid();
  this.connected = false;
  this.synced = false;
  this.shouldConnect = opt.connect !== false;
  this._marker = this; // origin tag: remote-applied updates carry this marker.
  this._updateHandler = null;
  this._awarenessHandler = null;
  this._mapChain = null;
  this._aMapChain = null;
  this._heartbeat = null;
  this._awareness = opt.awareness || null;

  var self = this;
  if(this._awareness){
    if(typeof this._awareness.on !== 'function' || typeof this._awareness.setLocalState !== 'function'){
      throw 'gun/lib/yjs.js: opt.awareness must be a y-protocols Awareness instance.'
    }
    this._awarenessHandler = function(change, origin){
      if(origin === self._marker){ return } // remote-applied, don't echo.
      self._publishAwareness();
    };
    this._awareness.on('update', this._awarenessHandler);
  }
  if(this.shouldConnect){ this.connect() }
}
GunYjsProvider.prototype = Object.create(Observable.prototype);
GunYjsProvider.prototype.constructor = GunYjsProvider;

GunYjsProvider.prototype.connect = function(){
  if(this.connected){ return }
  var self = this, Y = yjs();
  this.connected = true;
  this.synced = false;
  this.emit('status', {status: 'connecting'});

  var room = this.gun.get('yjs').get(this.name);

  // Make sure our slot exists BEFORE the initial read: Gun's once() never
  // fires for a missing node, and our own slot is what triggers the graph.
  this._publish();

  // Watch every client slot. .map().on() first re-emits existing slots,
  // which IS the initial sync; new writes arrive live afterwards.
  this._mapChain = room.get('c').map();
  this._mapChain.on(function(peer, id){
    self._onPeer(id, peer);
  });

  // Mark synced once the initial snapshot has been read from the graph.
  room.get('c').once(function(){
    self.synced = true;
    self.emit('sync', true);
    self.emit('status', {status: 'connected'});
  });

  // Local edits → publish our cumulative state.
  this._updateHandler = function(update, origin){
    if(origin === self._marker){ return } // remote-applied, don't echo.
    self._publish();
  };
  this.doc.on('update', this._updateHandler);

  // Awareness.
  if(this._awareness){
    var ap = awarenessProtocol(),
        aroom = room.get('a');
    this._aMapChain = aroom.map();
    this._aMapChain.on(function(peer, id){
      if(id === self.clientId || !peer || !peer.u){ return }
      ap.applyAwarenessUpdate(self._awareness, b64.decode(peer.u), self._marker);
    });
    this._publishAwareness();
    // Refresh so Gun's LWW timestamps stay fresh (y-protocols expiry: 30s).
    this._heartbeat = setInterval(function(){ self._publishAwareness() }, 15000);
  }
};

GunYjsProvider.prototype.disconnect = function(){
  if(!this.connected){ return }
  this.connected = false;
  this.synced = false;
  this.emit('sync', false);
  if(this._mapChain){ this._mapChain.off(); this._mapChain = null }
  if(this._aMapChain){ this._aMapChain.off(); this._aMapChain = null }
  if(this._updateHandler){ this.doc.off('update', this._updateHandler); this._updateHandler = null }
  if(this._heartbeat){ clearInterval(this._heartbeat); this._heartbeat = null }
  this.emit('status', {status: 'disconnected'});
};

GunYjsProvider.prototype.destroy = function(){
  this.disconnect();
  if(this._awareness && this._awarenessHandler){
    this._awareness.off('update', this._awarenessHandler);
    this._awarenessHandler = null;
  }
  this._events = {};
};

/* Publish this client's cumulative Yjs state (base64) + state vector. */
GunYjsProvider.prototype._publish = function(){
  if(!this.connected){ return }
  var Y = yjs();
  this.gun.get('yjs').get(this.name).get('c').get(this.clientId).put({
    u: b64.encode(Y.encodeStateAsUpdate(this.doc)),
    sv: b64.encode(Y.encodeStateVector(this.doc))
  });
};

/* Publish this client's awareness state (base64, y-protocols encoding). */
GunYjsProvider.prototype._publishAwareness = function(){
  if(!this.connected || !this._awareness){ return }
  // y-protocols only tracks meta for clients that have set local state;
  // publishing before that would crash encodeAwarenessUpdate.
  if(!this._awareness.getLocalState()){ return }
  var ap = awarenessProtocol();
  this.gun.get('yjs').get(this.name).get('a').get(this.clientId).put({
    u: b64.encode(ap.encodeAwarenessUpdate(this._awareness, [this.clientId]))
  });
};

/* Apply a peer's slot. Yjs applyUpdate is idempotent, so re-emissions and
 * LWW races are harmless: the union of all cumulative states converges. */
GunYjsProvider.prototype._onPeer = function(id, peer){
  if(id === this.clientId || !peer || !peer.u){ return }
  var Y = yjs();
  Y.applyUpdate(this.doc, b64.decode(peer.u), this._marker);
};

/* Chain helper: gun.yjs('room', doc, opt) -> provider. */
Gun.chain.yjs = Gun.chain.yjs || function(name, doc, opt){
  return new GunYjsProvider(this, name, doc, opt);
};

module.exports = GunYjsProvider;

}());