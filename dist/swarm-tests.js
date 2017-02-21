(function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);var f=new Error("Cannot find module '"+o+"'");throw f.code="MODULE_NOT_FOUND",f}var l=n[o]={exports:{}};t[o][0].call(l.exports,function(e){var n=t[o][1][e];return s(n?n:e)},l,l.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({1:[function(require,module,exports){
"use strict";

var env = require('./env');

function AsyncLoopbackConnection(url) {
    var m = url.match(/loopback:(\w+)/);
    if (!m) {
        throw new Error('invalid url');
    }
    this.id = m[1];
    this.lstn = {};
    this.queue = [];
    if (this.id in AsyncLoopbackConnection.pipes) {
        throw new Error('duplicate');
    }
    AsyncLoopbackConnection.pipes[this.id] = this;
    var pair = this.pair();
    if (pair && pair.queue.length) {
        pair.write();
    }
}
AsyncLoopbackConnection.pipes = {};

env.streams.loopback = AsyncLoopbackConnection;

AsyncLoopbackConnection.prototype.pair = function () {
    var pairId = this.id.match(/./g).reverse().join('');
    return AsyncLoopbackConnection.pipes[pairId];
};

AsyncLoopbackConnection.prototype.on = function (evname, fn) {
    if (evname in this.lstn) {
        throw new Error('multiple listeners not supported');
    }
    this.lstn[evname] = fn;
};

AsyncLoopbackConnection.prototype.receive = function (string) {
    this.lstn.data && this.lstn.data(string);
};

AsyncLoopbackConnection.prototype.write = function (obj) {
    var self = this;
    obj && self.queue.push(obj.toString());
    setTimeout(function () {
        var pair = self.pair();
        if (!pair) {
            return;
        }
        while (self.queue.length) {
            pair.receive(self.queue.shift());
        }
    }, 1);
};

AsyncLoopbackConnection.prototype.close = function () {
    delete AsyncLoopbackConnection.pipes[this.id];
    var pair = this.pair();
    pair && pair.close();
    this.lstn.close && this.lstn.close();
};

},{"./env":17}],2:[function(require,module,exports){
"use strict";


module.exports = {

    /**
     * Subscribe on collections entries' events
     * @param {function(Spec|string, Object, {deliver: function()})} callback
     * @this Set|Vector
     */
    onObjectEvent: function (callback) {
        this._proxy.owner = this;
        this._proxy.on(callback);
    },

    /**
     * Unsubscribe from collections entries' events
     * @param {function(*)} callback
     * @this Set|Vector
     */
    offObjectEvent: function (callback) {
        this._proxy.off(callback);
    },

    /**
     * Waits for collection to receive state from cache or uplink and then invokes passed callback
     *
     * @param {function()} callback
     * @this Set|Vector
     */
    onObjectStateReady: function (callback) { // TODO timeout ?
        var self = this;
        function checker() {
            var notInitedYet = self.filter(function (entry) {
                return !entry._version;
            });
            if (!notInitedYet.length) {
                // all entries are inited
                callback();
            } else {
                // wait for some entry not ready yet
                var randomIdx = (Math.random() * (notInitedYet.length - 1)) | 0;
                notInitedYet[randomIdx].once('init', checker);
            }
        }
        if (this._version) {
            checker();
        } else {
            this.once('init', checker);
        }
    }
};
},{}],3:[function(require,module,exports){
"use strict";

var env = require('./env');
var Spec = require('./Spec');
var Syncable = require('./Syncable');
var Pipe = require('./Pipe');
var SecondPreciseClock = require('./SecondPreciseClock');

/**
 * Host is (normally) a singleton object registering/coordinating
 * all the local Swarm objects, connecting them to appropriate
 * external uplinks, maintaining clocks, etc.
 * Host itself is not fully synchronized like a Model but still
 * does some event gossiping with peer Hosts.
 * @constructor
 */
function Host(id, ms, storage) {
    this.objects = {};
    this.sources = {};
    this.storage = storage;
    this._host = this; // :)
    this._lstn = [','];
    this._id = id;
    this._server = /^swarm~.*/.test(id);
    var clock_fn = env.clockType || SecondPreciseClock;
    this.clock = new clock_fn(this._id, ms||0);

    if (this.storage) {
        this.sources[this._id] = this.storage;
        this.storage._host = this;
    }
    delete this.objects[this.spec()];

    if (!env.multihost) {
        if (env.localhost) {
            throw new Error('use multihost mode');
        }
        env.localhost = this;
    }
}

Host.MAX_INT = 9007199254740992;
Host.MAX_SYNC_TIME = 60 * 60000; // 1 hour (milliseconds)
Host.HASH_POINTS = 3;

Host.hashDistance = function hashDistance(peer, obj) {
    if ((obj).constructor !== Number) {
        if (obj._id) {
            obj = obj._id;
        }
        obj = env.hashfn(obj);
    }
    if (peer._id) {
        peer = peer._id;
    }
    var dist = 4294967295;
    for (var i = 0; i < Host.HASH_POINTS; i++) {
        var hash = env.hashfn(peer._id + ':' + i);
        dist = Math.min(dist, hash ^ obj);
    }
    return dist;
};

module.exports = Syncable.extend(Host, {

    deliver: function (spec, val, repl) {
        if (spec.type() !== 'Host') {
            var typeid = spec.filter('/#');
            var obj = this.get(typeid);
            if (obj) {
                // TODO seeTimestamp()
                obj.deliver(spec, val, repl);
            }
        } else {
            this._super.deliver.apply(this, arguments);
        }
    },

    init: function (spec, val, repl) {

    },

    get: function (spec, callback) {
        if (spec && spec.constructor === Function && spec.prototype._type) {
            spec = '/' + spec.prototype._type;
        }
        spec = new Spec(spec);
        var typeid = spec.filter('/#');
        if (!typeid.has('/')) {
            throw new Error('invalid spec');
        }
        var o = typeid.has('#') && this.objects[typeid];
        if (!o) {
            var t = Syncable.types[spec.type()];
            if (!t) {
                throw new Error('type unknown: ' + spec);
            }
            o = new t(typeid, undefined, this);
            if (typeof(callback) === 'function') {
                o.on('.init', callback);
            }
        }
        return o;
    },

    addSource: function hostAddPeer(spec, peer) {
        //FIXME when their time is off so tell them so
        // if (false) { this.clockOffset; }
        var old = this.sources[peer._id];
        if (old) {
            old.deliver(this.newEventSpec('off'), '', this);
        }

        this.sources[peer._id] = peer;
        if (spec.op() === 'on') {
            peer.deliver(this.newEventSpec('reon'), this.clock.ms(), this);
        }
        for (var sp in this.objects) {
            this.objects[sp].checkUplink();
        }
    },

    neutrals: {
        /**
         * Host forwards on() calls to local objects to support some
         * shortcut notations, like
         *          host.on('/Mouse',callback)
         *          host.on('/Mouse.init',callback)
         *          host.on('/Mouse#Mickey',callback)
         *          host.on('/Mouse#Mickey.init',callback)
         *          host.on('/Mouse#Mickey!baseVersion',repl)
         *          host.on('/Mouse#Mickey!base.x',trackfn)
         * The target object may not exist beforehand.
         * Note that the specifier is actually the second 3sig parameter
         * (value). The 1st (spec) reflects this /Host.on invocation only.
         */
        on: function hostOn(spec, filter, lstn) {
            if (!filter) {
                // the subscriber needs "all the events"
                return this.addSource(spec, lstn);
            }

            if (filter.constructor === Function && filter.id) {
                filter = new Spec(filter.id, '/');
            } else if (filter.constructor === String) {
                filter = new Spec(filter, '.');
            }
            // either suscribe to this Host or to some other object
            if (!filter.has('/') || filter.type() === 'Host') {
                this._super._neutrals.on.call(this, spec, filter, lstn);
            } else {
                var objSpec = new Spec(filter);
                if (!objSpec.has('#')) {
                    throw new Error('no id to listen');
                }
                objSpec = objSpec.set('.on').set(spec.version(), '!');
                this.deliver(objSpec, filter, lstn);
            }
        },

        reon: function hostReOn(spec, ms, host) {
            if (spec.type() !== 'Host') {
                throw new Error('Host.reon(/NotHost.reon)');
            }
            this.clock.adjustTime(ms);
            this.addSource(spec, host);
        },

        off: function (spec, nothing, peer) {
            peer.deliver(peer.spec().add(this.time(), '!').add('.reoff'), '', this);
            this.removeSource(spec, peer);
        },

        reoff: function hostReOff(spec, nothing, peer) {
            this.removeSource(spec, peer);
        }

    }, // neutrals

    removeSource: function (spec, peer) {
        if (spec.type() !== 'Host') {
            throw new Error('Host.removeSource(/NoHost)');
        }

        if (this.sources[peer._id] !== peer) {
            console.error('peer unknown', peer._id); //throw new Error
            return;
        }
        delete this.sources[peer._id];
        for (var sp in this.objects) {
            var obj = this.objects[sp];
            if (obj.getListenerIndex(peer, true) > -1) {
                obj.off(sp, '', peer);
                obj.checkUplink(sp);
            }
        }
    },


    /**
     * Returns an unique Lamport timestamp on every invocation.
     * Swarm employs 30bit integer Unix-like timestamps starting epoch at
     * 1 Jan 2010. Timestamps are encoded as 5-char base64 tokens; in case
     * several events are generated by the same process at the same second
     * then sequence number is added so a timestamp may be more than 5
     * chars. The id of the Host (+user~session) is appended to the ts.
     */
    time: function () {
        var ts = this.clock.issueTimestamp();
        this._version = ts;
        return ts;
    },

    /**
     * Returns an array of sources (caches,storages,uplinks,peers)
     * a given replica should be subscribed to. This default
     * implementation uses a simple consistent hashing scheme.
     * Note that a client may be connected to many servers
     * (peers), so the uplink selection logic is shared.
     * @param {Spec} spec some object specifier
     * @returns {Array} array of currently available uplinks for specified object
     */
    getSources: function (spec) {
        var self = this,
            uplinks = [],
            mindist = 4294967295,
            rePeer = /^swarm~/, // peers, not clients
            target = env.hashfn(spec),
            closestPeer = null;

        if (rePeer.test(this._id)) {
            mindist = Host.hashDistance(this._id, target);
            closestPeer = this.storage;
        } else {
            uplinks.push(self.storage); // client-side cache
        }

        for (var id in this.sources) {
            if (!rePeer.test(id)) {
                continue;
            }
            var dist = Host.hashDistance(id, target);
            if (dist < mindist) {
                closestPeer = this.sources[id];
                mindist = dist;
            }
        }
        if (closestPeer) {
            uplinks.push(closestPeer);
        }
        return uplinks;
    },

    isUplinked: function () {
        for (var id in this.sources) {
            if (/^swarm~.*/.test(id)) {
                return true;
            }
        }
        return false;
    },

    isServer: function () {
        return this._server;
    },

    register: function (obj) {
        var spec = obj.spec();
        if (spec in this.objects) {
            return this.objects[spec];
        }
        this.objects[spec] = obj;
        return obj;
    },

    unregister: function (obj) {
        var spec = obj.spec();
        // TODO unsubscribe from the uplink - swarm-scale gc
        if (spec in this.objects) {
            delete this.objects[spec];
        }
    },

    // waits for handshake from stream
    accept: function (stream_or_url, pipe_env) {
        new Pipe(this, stream_or_url, pipe_env);
    },

    // initiate handshake with peer
    connect: function (stream_or_url, pipe_env) {
        var pipe = new Pipe(this, stream_or_url, pipe_env);
        pipe.deliver(new Spec('/Host#'+this._id+'!0.on'), '', this); //this.newEventSpec
        return pipe;
    },

    disconnect: function (id) {
        for (var peer_id in this.sources) {
            if (id && peer_id != id) {
                continue;
            }
            if (peer_id === this._id) {
                // storage
                continue;
            }
            var peer = this.sources[peer_id];
            // normally, .off is sent by a downlink
            peer.deliver(peer.spec().add(this.time(), '!').add('.off'));
        }
    },

    close: function (cb) {
        for(var id in this.sources) {
            if (id===this._id) {continue;}
            this.disconnect(id);
        }
        if (this.storage) {
            this.storage.close(cb);
        } else if (cb) {
            cb();
        }
    },

    checkUplink: function (spec) {
        //  TBD Host event relay + PEX
    }

});

},{"./Pipe":8,"./SecondPreciseClock":10,"./Spec":12,"./Syncable":14,"./env":17}],4:[function(require,module,exports){
"use strict";

var Spec = require('./Spec');

/** Pure logical-time Lamport clocks. */
var LamportClock = function (processId, initTime) {
    if (!Spec.reTok.test(processId)) {
        throw new Error('invalid process id: '+processId);
    }
    this.id = processId;
    // sometimes we assume our local clock has some offset
    this.seq = 0;
};

LamportClock.prototype.adjustTime = function () {
};

LamportClock.prototype.issueTimestamp = function time () {
    var base = Spec.int2base(this.seq++, 5);
    return base + '+' + this.id;
};

LamportClock.prototype.parseTimestamp = function parse (ts) {
    var m = ts.match(Spec.reTokExt);
    if (!m) {throw new Error('malformed timestamp: '+ts);}
    return {
        seq: Spec.base2int(m[1]),
        process: m[2]
    };
};

/** Lamport partial order  imperfect semi-logical*/
LamportClock.prototype.checkTimestamp = function see (ts) {
    var parsed = this.parseTimestamp(ts);
    if (parsed.seq >= this.seq) {
        this.seq = parsed.seq + 1;
    }
    return true;
};

LamportClock.prototype.time2date = function () {
    return undefined;
};

module.exports = LamportClock;

},{"./Spec":12}],5:[function(require,module,exports){
"use strict";

var Spec = require('./Spec');

/**LongSpec is a Long Specifier, i.e. a string of quant+id tokens that may be
 * indeed very (many megabytes) long.  Ids are compressed using
 * dynamic dictionaries (codebooks) or "unicode numbers" (base-32768
 * encoding utilizing Unicode symbols as quasi-binary).  Unicode
 * numbers are particularly handy for encoding timestamps.  LongSpecs
 * may be assigned shared codebooks (2nd parameter); a codebook is an
 * object containing encode/decode tables and some stats, e.g.
 * {en:{'/Type':'/T'}, de:{'/T':'/Type'}}. It is OK to pass an empty object as
 * a codebook; it gets initialized automatically).  */
var LongSpec = function (spec, codeBook) {
    var cb = this.codeBook = codeBook || {en:{},de:{}};
    if (!cb.en) { cb.en = {}; }
    if (!cb.de) { // revert en to make de
        cb.de = {};
        for(var tok in cb.en) {
            cb.de[cb.en[tok]] = tok;
        }
    }
    if (!cb.lastCodes) {
        cb.lastCodes = {'/':0x30,'#':0x30,'!':0x30,'.':0x30,'+':0x30};
    }
    // For a larger document, a single LongSpec may be some megabytes long.
    // As we don't want to rewrite those megabytes on every keypress, we
    // divide data into chunks.
    this.chunks = [];
    this.chunkLengths = [];
    if (spec) {
        this.append(spec);
    }
};

LongSpec.reQTokEn = /([/#\!\.\+])([0-\u802f]+)/g;
LongSpec.reQTok = new RegExp('([/#\\.!\\*\\+])(=)'.replace(/=/g, Spec.rT), 'g');
LongSpec.rTEn = '[0-\\u802f]+';
LongSpec.reQTokExtEn = new RegExp
    ('([/#\\.!\\*])((=)(?:\\+(=))?)'.replace(/=/g, LongSpec.rTEn), 'g');

/** Well, for many-MB LongSpecs this may take some time. */
LongSpec.prototype.toString = function () {
    var ret = [];
    for(var i = this.iterator(); !i.end(); i.next()){
        ret.push(i.decode());
    }
    return ret.join('');
};

LongSpec.prototype.length = function () { // TODO .length ?
    var len = 0;
    for(var i=0; i<this.chunks.length; i++) {
        len += this.chunkLengths[i];
    }
    return len;
};

LongSpec.prototype.charLength = function () {
    var len = 0;
    for(var i=0; i<this.chunks.length; i++) {
        len += this.chunks[i].length;
    }
    return len;
};

//   T O K E N  C O M P R E S S I O N

LongSpec.prototype.allocateCode = function (tok) {
    var quant = tok.charAt(0);
    //if (Spec.quants.indexOf(quant)===-1) {throw new Error('invalid token');}
    var en, cb = this.codeBook, lc = cb.lastCodes;
    if (lc[quant]<'z'.charCodeAt(0)) { // pick a nice letter
        for(var i=1; !en && i<tok.length; i++) {
            var x = tok.charAt(i), e = quant+x;
            if (!cb.de[e]) {  en = e;  }
        }
    }
    while (!en && lc[quant]<0x802f) {
        var y = String.fromCharCode(lc[quant]++);
        var mayUse = quant + y;
        if ( ! cb.en[mayUse] ) {  en = mayUse;  }
    }
    if (!en) {
        if (tok.length<=3) {
            throw new Error("out of codes");
        }
        en = tok;
    }
    cb.en[tok] = en;
    cb.de[en] = tok;
    return en;
};

//  F O R M A T  C O N V E R S I O N


/** Always 2-char base2^15 coding for an int (0...2^30-1) */
LongSpec.int2uni = function (i) {
    if (i<0 || i>0x7fffffff) { throw new Error('int is out of range'); }
    return String.fromCharCode( 0x30+(i>>15), 0x30+(i&0x7fff) );
};

LongSpec.uni2int = function (uni) {
    if (!/^[0-\u802f]{2}$/.test(uni)) {
        throw new Error('invalid unicode number') ;
    }
    return ((uni.charCodeAt(0)-0x30)<<15) | (uni.charCodeAt(1)-0x30);
};

//  I T E R A T O R S

/*  Unfortunately, LongSpec cannot be made a simple array because tokens are
    not fixed-width in the general case. Some tokens are dictionary-encoded
    into two-symbol segments, e.g. ".on" --> ".o". Other tokens may need 6
    symbols to encode, e.g. "!timstse+author~ssn" -> "!tss+a".
    Also, iterators opportuniatically use sequential compression. Namely,
    tokens that differ by +1 are collapsed into quant-only sequences:
    "!abc+s!abd+s" -> "!abc+s!"
    So, locating and iterating becomes less-than-trivial. Raw string offsets
    better not be exposed in the external interface; hence, we need iterators.

    {
        offset:5,       // char offset in the string (chunk)
        index:1,        // index of the entry (token)
        en: "!",        // the actual matched token (encoded)
        chunk:0,        // index of the chunk
        de: "!timst00+author~ssn", // decoded token
        seqstart: "!ts0+a", // first token of the sequence (encoded)
        seqoffset: 3    // offset in the sequence
    }
*/
LongSpec.Iterator = function Iterator (owner, index) {
    this.owner = owner;         // our LongSpec
    /*this.chunk = 0;             // the chunk we are in
    this.index = -1;            // token index (position "before the 1st token")
    this.chunkIndex = -1;       // token index within the chunk
    this.prevFull = undefined;  // previous full (non-collapsed) token
    //  seqStart IS the previous match or prev match is trivial
    this.prevCollapsed = 0;
    this.match = null;
    //this.next();*/
    this.skip2chunk(0);
    if (index) {
        if (index.constructor===LongSpec.Iterator) {
            index = index.index;
        }
        this.skip(index);
    }
};


// also matches collapsed quant-only tokens
LongSpec.Iterator.reTok = new RegExp
    ('([/#\\.!\\*])((=)(?:\\+(=))?)?'.replace(/=/g, LongSpec.rTEn), 'g');


/* The method converts a (relatively) verbose Base64 specifier into an
 * internal compressed format.  Compressed tokens are also
 * variable-length; the length of the token depends on the encoding
 * method used.
 * 1 unicode symbol: dictionary-encoded (up to 2^15 entries for each quant),
 * 2 symbols: simple timestamp base-2^15 encoded,
 * 3 symbols: timestamp+seq base-2^15,
 * 4 symbols: long-number base-2^15,
 * 5 symbols and more: unencoded original (fallback).
 * As long as two sequential unicoded entries differ by +1 in the body
 * of the token (quant and extension being the same), we use sequential
 * compression. The token is collapsed (only the quant is left).
 * */
LongSpec.Iterator.prototype.encode = function encode (de) {
    var re = Spec.reQTokExt;
    re.lastIndex = 0;
    var m=re.exec(de); // this one is de
    if (!m || m[0].length!==de.length) {throw new Error('malformed token: '+de);}
    var tok=m[0], quant=m[1], body=m[3], ext=m[4];
    var pm = this.prevFull; // this one is en
    var prevTok, prevQuant, prevBody, prevExt;
    var enBody, enExt;
    if (pm) {
        prevTok=pm[0], prevQuant=pm[1], prevBody=pm[3], prevExt=pm[4]?'+'+pm[4]:undefined;
    }
    if (ext) {
        enExt = this.owner.codeBook.en['+'+ext] || this.owner.allocateCode('+'+ext);
    }
    var maySeq = pm && quant===prevQuant && enExt===prevExt;
    var haveSeq=false, seqBody = '';
    var int1, int2, uni1, uni2;
    //var expected = head + (counter===-1?'':Spec.int2base(counter+inc,1)) + tail;
    if ( body.length<=4 ||          // TODO make it a switch
         (quant in LongSpec.quants2code) ||
         (tok in this.owner.codeBook.en) ) {  // 1 symbol by the codebook

        enBody = this.owner.codeBook.en[quant+body] ||
                 this.owner.allocateCode(quant+body);
        enBody = enBody.substr(1); // FIXME separate codebooks 4 quants
        if (maySeq) {// seq coding for dictionary-coded
            seqBody = enBody;
        }
    } else if (body.length===5) { // 2-symbol base-2^15
        var int = Spec.base2int(body);
        enBody = LongSpec.int2uni(int);
        if (maySeq && prevBody.length===2) {
            seqBody = LongSpec.int2uni(int-this.prevCollapsed-1);
        }
    } else if (body.length===7) { // 3-symbol base-2^15
        int1 = Spec.base2int(body.substr(0,5));
        int2 = Spec.base2int(body.substr(5,2));
        uni1 = LongSpec.int2uni(int1);
        uni2 = LongSpec.int2uni(int2).charAt(1);
        enBody = uni1 + uni2;
        if (maySeq && prevBody.length===3) {
            seqBody = uni1 + LongSpec.int2uni(int2-this.prevCollapsed-1).charAt(1);
        }
    } else if (body.length===10) { // 4-symbol 60-bit long number
        int1 = Spec.base2int(body.substr(0,5));
        int2 = Spec.base2int(body.substr(5,5));
        uni1 = LongSpec.int2uni(int1);
        uni2 = LongSpec.int2uni(int2);
        enBody = uni1 + uni2;
        if (maySeq && prevBody.length===4) {
            seqBody = uni1+LongSpec.int2uni(int2-this.prevCollapsed-1);
        }
    } else { // verbatim
        enBody = body;
        seqBody = enBody;
    }
    haveSeq = seqBody===prevBody;
    return haveSeq ? quant : quant+enBody+(enExt||'');
};
LongSpec.quants2code = {'/':1,'.':1};

/** Decode a compressed specifier back into base64. */
LongSpec.Iterator.prototype.decode = function decode () {
    if (this.match===null) { return undefined; }
    var quant = this.match[1];
    var body = this.match[3];
    var ext = this.match[4];
    var pm=this.prevFull, prevTok, prevQuant, prevBody, prevExt;
    var int1, int2, base1, base2;
    var de = quant;
    if (pm) {
        prevTok=pm[0], prevQuant=pm[1], prevBody=pm[3], prevExt=pm[4];
    }
    if (!body) {
        if (prevBody.length===1) {
            body = prevBody;
        } else {
            var l_1 = prevBody.length-1;
            var int = prevBody.charCodeAt(l_1);
            body = prevBody.substr(0,l_1) + String.fromCharCode(int+this.prevCollapsed+1);
        }
        ext = prevExt;
    }
    switch (body.length) {
        case 1:
            de += this.owner.codeBook.de[quant+body].substr(1); // TODO sep codebooks
            break;
        case 2:
            int1 = LongSpec.uni2int(body);
            base1 = Spec.int2base(int1,5);
            de += base1;
            break;
        case 3:
            int1 = LongSpec.uni2int(body.substr(0,2));
            int2 = LongSpec.uni2int('0'+body.charAt(2));
            base1 = Spec.int2base(int1,5);
            base2 = Spec.int2base(int2,2);
            de += base1 + base2;
            break;
        case 4:
            int1 = LongSpec.uni2int(body.substr(0,2));
            int2 = LongSpec.uni2int(body.substr(2,2));
            base1 = Spec.int2base(int1,5);
            base2 = Spec.int2base(int2,5);
            de += base1 + base2;
            break;
        default:
            de += body;
            break;
    }
    if (ext) {
        var deExt = this.owner.codeBook.de['+'+ext];
        de += deExt;
    }
    return de;
};


LongSpec.Iterator.prototype.next = function ( ) {

    if (this.end()) {return;}

    var re = LongSpec.Iterator.reTok;
    re.lastIndex = this.match ? this.match.index+this.match[0].length : 0;
    var chunk = this.owner.chunks[this.chunk];

    if (chunk.length===re.lastIndex) {
        this.chunk++;
        this.chunkIndex = 0;
        if (this.match && this.match[0].length>0) {
            this.prevFull = this.match;
            this.prevCollapsed = 0;
        } else if (this.match) {
            this.prevCollapsed++;
        } else { // empty
            this.prevFull = undefined;
            this.prevCollapsed = 0;
        }
        this.match = null;
        this.index ++;
        if (this.end()) {return;}
    }

    if (this.match[0].length>1) {
        this.prevFull = this.match;
        this.prevCollapsed = 0;
    } else {
        this.prevCollapsed++;
    }

    this.match = re.exec(chunk);
    this.index++;
    this.chunkIndex++;

    return this.match[0];
};


LongSpec.Iterator.prototype.end = function () {
    return this.match===null && this.chunk===this.owner.chunks.length;
};


LongSpec.Iterator.prototype.skip = function ( count ) {
    // TODO may implement fast-skip of seq-compressed spans
    var lengths = this.owner.chunkLengths, chunks = this.owner.chunks;
    count = count || 1;
    var left = count;
    var leftInChunk = lengths[this.chunk]-this.chunkIndex;
    if ( leftInChunk <= count ) { // skip chunks
        left -= leftInChunk; // skip the current chunk
        var c=this.chunk+1;    // how many extra chunks to skip
        while (left>chunks[c] && c<chunks.length) {
            left-=chunks[++c];
        }
        this.skip2chunk(c);
    }
    if (this.chunk<chunks.length) {
        while (left>0) {
            this.next();
            left--;
        }
    }
    return count - left;
};

/** Irrespectively of the current state of the iterator moves it to the
  * first token in the chunk specified; chunk===undefined moves it to
  * the end() position (one after the last token). */
LongSpec.Iterator.prototype.skip2chunk = function ( chunk ) {
    var chunks = this.owner.chunks;
    if (chunk===undefined) {chunk=chunks.length;}
    this.index = 0;
    for(var c=0; c<chunk; c++) { // TODO perf pick the current value
        this.index += this.owner.chunkLengths[c];
    }
    this.chunkIndex = 0;
    this.chunk = chunk;
    var re = LongSpec.Iterator.reTok;
    if ( chunk < chunks.length ) {
        re.lastIndex = 0;
        this.match = re.exec(chunks[this.chunk]);
    } else {
        this.match = null;
    }
    if (chunk>0) { // (1) chunks must not be empty; (2) a chunk starts with a full token
        var prev = chunks[chunk-1];
        var j = 0;
        while (Spec.quants.indexOf(prev.charAt(prev.length-1-j)) !== -1) { j++; }
        this.prevCollapsed = j;
        var k = 0;
        while (Spec.quants.indexOf(prev.charAt(prev.length-1-j-k))===-1) { k++; }
        re.lastIndex = prev.length-1-j-k;
        this.prevFull = re.exec(prev);
    } else {
        this.prevFull = undefined;
        this.prevCollapsed = 0;
    }
};

LongSpec.Iterator.prototype.token = function () {
    return this.decode();
};

/*LongSpec.Iterator.prototype.de = function () {
    if (this.match===null) {return undefined;}
    return this.owner.decode(this.match[0],this.prevFull?this.prevFull[0]:undefined,this.prevCollapsed);
};*/

/*LongSpec.Iterator.prototype.insertDe = function (de) {
    var en = this.owner.encode(de,this.prevFull?this.prevFull[0]:undefined,this.prevCollapsed);
    this.insert(en);
};*/


/** As sequential coding is incapsulated in LongSpec.Iterator, inserts are
  * done by Iterator as well. */
LongSpec.Iterator.prototype.insert = function (de) { // insertBefore

    var insStr = this.encode(de);

    var brokenSeq = this.match && this.match[0].length===1;

    var re = LongSpec.Iterator.reTok;
    var chunks = this.owner.chunks, lengths = this.owner.chunkLengths;
    if (this.chunk==chunks.length) { // end(), append
        if (chunks.length>0) {
            var ind = this.chunk - 1;
            chunks[ind] += insStr;
            lengths[ind] ++;
        } else {
            chunks.push(insStr);
            lengths.push(1);
            this.chunk++;
        }
    } else {
        var chunkStr = chunks[this.chunk];
        var preEq = chunkStr.substr(0, this.match.index);
        var postEq = chunkStr.substr(this.match.index);
        if (brokenSeq) {
            var me = this.token();
            this.prevFull = undefined;
            var en = this.encode(me);
            chunks[this.chunk] = preEq + insStr + en + postEq.substr(1);
            re.lastIndex = preEq.length + insStr.length;
            this.match = re.exec(chunks[this.chunk]);
        } else {
            chunks[this.chunk] = preEq + insStr + /**/ postEq;
            this.match.index += insStr.length;
        }
        lengths[this.chunk] ++;
        this.chunkIndex ++;
    }
    this.index ++;
    if (insStr.length>1) {
        re.lastIndex = 0;
        this.prevFull = re.exec(insStr);
        this.prevCollapsed = 0;
    } else {
        this.prevCollapsed++;
    }

    // may split chunks
    // may join chunks
};

LongSpec.Iterator.prototype.insertBlock = function (de) { // insertBefore
    var re = Spec.reQTokExt;
    var toks = de.match(re).reverse(), tok;
    while (tok=toks.pop()) {
        this.insert(tok);
    }
};

LongSpec.Iterator.prototype.erase = function (count) {
    if (this.end()) {return;}
    count = count || 1;
    var chunks = this.owner.chunks;
    var lengths = this.owner.chunkLengths;
    // remember offsets
    var fromChunk = this.chunk;
    var fromOffset = this.match.index;
    var fromChunkIndex = this.chunkIndex; // TODO clone USE 2 iterators or i+c

    count = this.skip(count); // checked for runaway skip()
    // the iterator now is at the first-after-erased pos

    var tillChunk = this.chunk;
    var tillOffset = this.match ? this.match.index : 0; // end()

    var collapsed = this.match && this.match[0].length===1;

    // splice strings, adjust indexes
    if (fromChunk===tillChunk) {
        var chunk = chunks[this.chunk];
        var pre = chunk.substr(0,fromOffset);
        var post = chunk.substr(tillOffset);
        if (collapsed) { // sequence is broken now; needs expansion
            post = this.token() + post.substr(1);
        }
        chunks[this.chunk] = pre + post;
        lengths[this.chunk] -= count;
        this.chunkIndex -= count;
    } else {  // FIXME refac, more tests (+wear)
        if (fromOffset===0) {
            fromChunk--;
        } else {
            chunks[fromChunk] = chunks[fromChunk].substr(0,fromOffset);
            lengths[fromChunk] = fromChunkIndex;
        }
        var midChunks = tillChunk - fromChunk - 1;
        if (midChunks) { // wipe'em out
            //for(var c=fromChunk+1; c<tillChunk; c++) ;
            chunks.splice(fromChunk+1,midChunks);
            lengths.splice(fromChunk+1,midChunks);
        }
        if (tillChunk<chunks.length && tillOffset>0) {
            chunks[tillChunk] = chunks[tillChunk].substr(this.match.index);
            lengths[tillChunk] -= this.chunkIndex;
            this.chunkIndex = 0;
        }
    }
    this.index -= count;

};


LongSpec.Iterator.prototype.clone = function () {
    var copy = new LongSpec.Iterator(this.owner);
    copy.chunk = this.chunk;
    copy.match = this.match;
    copy.index = this.index;
};

//  L O N G S P E C  A P I

LongSpec.prototype.iterator = function (index) {
    return new LongSpec.Iterator(this,index);
};

LongSpec.prototype.end = function () {
    var e = new LongSpec.Iterator(this);
    e.skip2chunk(this.chunks.length);
    return e;
};

/** Insert a token at a given position. */
LongSpec.prototype.insert = function (tok, i) {
    var iter = i.constructor===LongSpec.Iterator ? i : this.iterator(i);
    iter.insertBlock(tok);
};

LongSpec.prototype.tokenAt = function (pos) {
    var iter = this.iterator(pos);
    return iter.token();
};

LongSpec.prototype.indexOf = function (tok, startAt) {
    var iter = this.find(tok,startAt);
    return iter.end() ? -1 : iter.index;
};

/*LongSpec.prototype.insertAfter = function (tok, i) {
    LongSpec.reQTokExtEn.lastIndex = i;
    var m = LongSpec.reQTokExtEn.exec(this.value);
    if (m.index!==i) { throw new Error('incorrect position'); }
    var splitAt = i+m[0].length;
    this.insertBefore(tok,splitAt);
};*/

LongSpec.prototype.add = function ls_add (spec) {
    var pos = this.end();
    pos.insertBlock(spec);
};
LongSpec.prototype.append = LongSpec.prototype.add;

/** The method finds the first occurence of a token, returns an
 * iterator.  While the internal format of an iterator is kind of
 * opaque, and generally is not recommended to rely on, that is
 * actually a regex match array. Note that it contains encoded tokens.
 * The second parameter is the position to start scanning from, passed
 * either as an iterator or an offset. */
LongSpec.prototype.find = function (tok, startIndex) {
    //var en = this.encode(tok).toString(); // don't split on +
    var i = this.iterator(startIndex);
    while (!i.end()) {
        if (i.token()===tok) {
            return i;
        }
        i.next();
    }
    return i;
};

module.exports = LongSpec;

},{"./Spec":12}],6:[function(require,module,exports){
"use strict";

var Spec = require('./Spec');

/** It is not always necessary to have second-precise timestamps.
  * Going with minute-precise allows to fit timestamp values
  * into 30 bits (5 base64, 2 unicode chars).
  * More importantly, such timestamps increase incrementally for
  * short bursts of events (e.g. user typing). That allows
  * for sequence-coding optimizations in LongSpec.
  * In case processes generate more than 64 events a minute,
  * which is not unlikely, the optimization fails as we add
  * 12-bit seq (2 base64, 1 unicode). */
var MinutePreciseClock = function (processId, timeOffsetMs) {
    if (!Spec.reTok.test(processId)) {
        throw new Error('invalid process id: '+processId);
    }
    this.id = processId;
    // sometimes we assume our local clock has some offset
    this.clockOffsetMs = 0;
    this.lastIssuedTimestamp = '';
    // although we try hard to use wall clock time, we must
    // obey Lamport logical clock rules, in particular our
    // timestamps must be greater than any other timestamps
    // previously seen
    this.lastTimeSeen = 0;
    this.lastSeqSeen = 0;
    if (timeOffsetMs) {
        this.clockOffsetMs = timeOffsetMs;
    }
};

var epochDate = new Date("Wed, 01 Jan 2014 00:00:00 GMT");
MinutePreciseClock.EPOCH = epochDate.getTime();

MinutePreciseClock.prototype.adjustTime = function (trueMs) {
    // TODO use min historical offset
    var localTime = new Date().getTime();
    var clockOffsetMs = trueMs - localTime;
    this.clockOffsetMs = clockOffsetMs;
};

MinutePreciseClock.prototype.minutes = function () {
    var millis = new Date().getTime();
    millis -= MinutePreciseClock.EPOCH;
    millis += this.clockOffsetMs;
    return (millis/60000) | 0;
};

MinutePreciseClock.prototype.issueTimestamp = function () {
    var time = this.minutes();
    if (this.lastTimeSeen>time) { time = this.lastTimeSeen; }
    if (time>this.lastTimeSeen) { this.lastSeqSeen = -1; }
    this.lastTimeSeen = time;
    var seq = ++this.lastSeqSeen;
    if (seq>=(1<<18)) {throw new Error('max event freq is 4000Hz');}

    var baseTime = Spec.int2base(time, 4), baseSeq;
    if (seq<64) {
        baseSeq = Spec.int2base(seq, 1);
    } else {
        baseSeq = Spec.int2base(seq, 3);
    }

    this.lastIssuedTimestamp = baseTime + baseSeq + '+' + this.id;
    return this.lastIssuedTimestamp;
};

MinutePreciseClock.prototype.parseTimestamp = function parse (ts) {
    var m = ts.match(Spec.reTokExt);
    if (!m) {throw new Error('malformed timestamp: '+ts);}
    var timeseq=m[1]; //, process=m[2];
    var time = timeseq.substr(0,4), seq = timeseq.substr(4);
    if (seq.length!==1 && seq.length!==3) {
        throw new Error('malformed timestamp value: '+timeseq);
    }
    return {
        time: Spec.base2int(time),
        seq: Spec.base2int(seq)
    };
};

MinutePreciseClock.prototype.checkTimestamp = function see (ts) {
    if (ts<this.lastIssuedTimestamp) { return true; }
    var parsed = this.parseTimestamp(ts);
    if (parsed.time<this.lastTimeSeen) { return true; }
    var min = this.minutes();
    if (parsed.time>min+1) { return false; } // bad clocks somewhere
    this.lastTimeSeen = parsed.time;
    this.lastSeqSeen = parsed.seq;
    return true;
};


MinutePreciseClock.prototype.time2date = function () {
    // parse etc
};

module.exports = MinutePreciseClock;

},{"./Spec":12}],7:[function(require,module,exports){
"use strict";

var Spec = require('./Spec');
var Syncable = require('./Syncable');

/**
 * Model (LWW key-value object)
 * @param idOrState
 * @constructor
 */
function Model(idOrState) {
    var ret = Model._super.apply(this, arguments);
    /// TODO: combine with state push, make clean
    if (ret === this && idOrState && idOrState.constructor !== String && !Spec.is(idOrState)) {
        this.deliver(this.spec().add(this._id, '!').add('.set'), idOrState);
    }
}

module.exports = Syncable.extend(Model, {
    defaults: {
        _oplog: Object
    },
    /**  init modes:
     *    1  fresh id, fresh object
     *    2  known id, stateless object
     *    3  known id, state boot
     */
    neutrals: {
        on: function (spec, base, repl) {
            //  support the model.on('field',callback_fn) pattern
            if (typeof(repl) === 'function' &&
                    typeof(base) === 'string' &&
                    (base in this.constructor.defaults)) {
                var stub = {
                    fn: repl,
                    key: base,
                    self: this,
                    _op: 'set',
                    deliver: function (spec, val, src) {
                        if (this.key in val) {
                            this.fn.call(this.self, spec, val, src);
                        }
                    }
                };
                repl = stub;
                base = '';
            }
            // this will delay response if we have no state yet
            Syncable._pt._neutrals.on.call(this, spec, base, repl);
        },

        off: function (spec, base, repl) {
            var ls = this._lstn;
            if (typeof(repl) === 'function') { // TODO ugly
                for (var i = 0; i < ls.length; i++) {
                    if (ls[i] && ls[i].fn === repl && ls[i].key === base) {
                        repl = ls[i];
                        break;
                    }
                }
            }
            Syncable._pt._neutrals.off.apply(this, arguments);
        }

    },

    // TODO remove unnecessary value duplication
    packState: function (state) {
    },
    unpackState: function (state) {
    },
    /**
     * Removes redundant information from the log; as we carry a copy
     * of the log in every replica we do everythin to obtain the minimal
     * necessary subset of it.
     * As a side effect, distillLog allows up to handle some partial
     * order issues (see _ops.set).
     * @see Model.ops.set
     * @returns {*} distilled log {spec:true}
     */
    distillLog: function () {
        // explain
        var sets = [],
            cumul = {},
            heads = {},
            spec;
        for (var s in this._oplog) {
            spec = new Spec(s);
            //if (spec.op() === 'set') {
            sets.push(spec);
            //}
        }
        sets.sort();
        for (var i = sets.length - 1; i >= 0; i--) {
            spec = sets[i];
            var val = this._oplog[spec],
                notempty = false;
            for (var field in val) {
                if (field in cumul) {
                    delete val[field];
                } else {
                    notempty = cumul[field] = val[field]; //store last value of the field
                }
            }
            var source = spec.source();
            notempty || (heads[source] && delete this._oplog[spec]);
            heads[source] = true;
        }
        return cumul;
    },

    ops: {
        /**
         * This barebones Model class implements just one kind of an op:
         * set({key:value}). To implment your own ops you need to understand
         * implications of partial order as ops may be applied in slightly
         * different orders at different replicas. This implementation
         * may resort to distillLog() to linearize ops.
         */
        set: function (spec, value, repl) {
            var version = spec.version(),
                vermet = spec.filter('!.').toString();
            if (version < this._version.substr(1)) {
                this._oplog[vermet] = value;
                this.distillLog(); // may amend the value
                value = this._oplog[vermet];
            }
            value && this.apply(value);
        }
    },

    fill: function (key) { // TODO goes to Model to support references
        if (!this.hasOwnProperty(key)) {
            throw new Error('no such entry');
        }

        //if (!Spec.is(this[key]))
        //    throw new Error('not a specifier');
        var spec = new Spec(this[key]).filter('/#');
        if (spec.pattern() !== '/#') {
            throw new Error('incomplete spec');
        }

        this[key] = this._host.get(spec);
        /* TODO new this.refType(id) || new Swarm.types[type](id);
         on('init', function(){
         self.emit('fill',key,this)
         self.emit('full',key,this)
         });*/
    },

    /**
     * Generate .set operation after some of the model fields were changed
     * TODO write test for Model.save()
     */
    save: function () {
        var cumul = this.distillLog(),
            changes = {},
            pojo = this.pojo(),
            field;
        for (field in pojo) {
            if (this[field] !== cumul[field]) {// TODO nesteds
                changes[field] = this[field];
            }
        }
        for (field in cumul) {
            if (!(field in pojo)) {
                changes[field] = null; // JSON has no undefined
            }
        }
        this.set(changes);
    },

    validate: function (spec, val) {
        if (spec.op() !== 'set') {
            return '';
        } // no idea
        for (var key in val) {
            if (!Syncable.reFieldName.test(key)) {
                return 'bad field name';
            }
        }
        return '';
    }

});

// Model may have reactions for field changes as well as for 'real' ops/events
// (a field change is a .set operation accepting a {field:newValue} map)
module.exports.addReaction = function (methodOrField, fn) {
    var proto = this.prototype;
    if (typeof (proto[methodOrField]) === 'function') { // it is a field name
        return Syncable.addReaction.call(this, methodOrField, fn);
    } else {
        var wrapper = function (spec, val) {
            if (methodOrField in val) {
                fn.apply(this, arguments);
            }
        };
        wrapper._rwrap = true;
        return Syncable.addReaction.call(this, 'set', wrapper);
    }
};

},{"./Spec":12,"./Syncable":14}],8:[function(require,module,exports){
"use strict";

var env = require('./env');
var Spec = require('./Spec');

/**
 * A "pipe" is a channel to a remote Swarm Host. Pipe's interface
 * mocks a Host except all calls are serialized and sent to the
 * *stream*; any arriving data is parsed and delivered to the
 * local host. The *stream* must support an interface of write(),
 * end() and on('open'|'data'|'close'|'error',fn).  Instead of a
 * *stream*, the caller may supply an *uri*, so the Pipe will
 * create a stream and connect/reconnect as necessary.
 */

function Pipe(host, stream, opts) {
    var self = this;
    self.opts = opts || {};
    if (!stream || !host) {
        throw new Error('new Pipe(host,stream[,opts])');
    }
    self._id = null;
    self.host = host;
    // uplink/downlink state flag;
    //  true: this side initiated handshake >.on <.reon
    //  false: this side received handshake <.on >.reon
    //  undefined: nothing sent/received OR had a .reoff
    this.isOnSent = undefined;
    this.reconnectDelay = self.opts.reconnectDelay || 1000;
    self.serializer = self.opts.serializer || JSON;
    self.katimer = null;
    self.send_timer = null;
    self.lastSendTS = self.lastRecvTS = self.time();
    self.bundle = {};
    // don't send immediately, delay to bundle more messages
    self.delay = self.opts.delay || -1;
    //self.reconnectDelay = self.opts.reconnectDelay || 1000;
    if (typeof(stream.write) !== 'function') { // TODO nicer
        var url = stream.toString();
        var m = url.match(/(\w+):.*/);
        if (!m) {
            throw new Error('invalid url ' + url);
        }
        var proto = m[1].toLowerCase();
        var fn = env.streams[proto];
        if (!fn) {
            throw new Error('protocol not supported: ' + proto);
        }
        self.url = url;
        stream = new fn(url);
    }
    self.connect(stream);
}

module.exports = Pipe;
//env.streams = {};
Pipe.TIMEOUT = 60000; //ms

Pipe.prototype.connect = function pc(stream) {
    var self = this;
    self.stream = stream;

    self.stream.on('data', function onMsg(data) {
        data = data.toString();
        env.trace && env.log(dotIn, data, this, this.host);
        self.lastRecvTS = self.time();
        var json = self.serializer.parse(data);
        try {
            self._id ? self.parseBundle(json) : self.parseHandshake(json);
        } catch (ex) {
            console.error('error processing message', ex, ex.stack);
            //this.deliver(this.host.newEventSpec('error'), ex.message);
            this.close();
        }
        self.reconnectDelay = self.opts.reconnectDelay || 1000;
    });

    self.stream.on('close', function onConnectionClosed(reason) {
        self.stream = null; // needs no further attention
        self.close("stream closed");
    });

    self.stream.on('error', function (err) {
        self.close('stream error event: ' + err);
    });

    self.katimer = setInterval(self.keepAliveFn.bind(self), (Pipe.TIMEOUT / 4 + Math.random() * 100) | 0);

    // NOPE client only finally, initiate handshake
    // self.host.connect(self);

};

Pipe.prototype.keepAliveFn = function () {
    var now = this.time(),
        sinceRecv = now - this.lastRecvTS,
        sinceSend = now - this.lastSendTS;
    if (sinceSend > Pipe.TIMEOUT / 2) {
        this.sendBundle();
    }
    if (sinceRecv > Pipe.TIMEOUT) {
        this.close("stream timeout");
    }
};

Pipe.prototype.parseHandshake = function ph(handshake) {
    var spec, value, key;
    for (key in handshake) {
        spec = new Spec(key);
        value = handshake[key];
        break; // 8)-
    }
    if (!spec) {
        throw new Error('handshake has no spec');
    }
    if (spec.type() !== 'Host') {
        env.warn("non-Host handshake");
    }
    if (spec.id() === this.host._id) {
        throw new Error('self hs');
    }
    this._id = spec.id();
    var op = spec.op();
    var evspec = spec.set(this.host._id, '#');

    if (op in {on: 1, reon: 1, off: 1, reoff: 1}) {// access denied TODO
        this.host.deliver(evspec, value, this);
    } else {
        throw new Error('invalid handshake');
    }
};

/**
 * Close the underlying stream.
 * Schedule new Pipe creation (when error passed).
 * note: may be invoked multiple times
 * @param {Error|string} error
 */
Pipe.prototype.close = function pc(error) {
    env.log(dotClose, error ? 'error: ' + error : 'correct', this, this.host);
    if (error && this.host && this.url) {
        var uplink_uri = this.url,
            host = this.host,
            pipe_opts = this.opts;
        //reconnect delay for next disconnection
        pipe_opts.reconnectDelay = Math.min(30000, this.reconnectDelay << 1);
        // schedule a retry
        setTimeout(function () {
            host.connect(uplink_uri, pipe_opts);
        }, this.reconnectDelay);

        this.url = null; //to prevent second reconnection timer
    }
    if (this.host) {
        if (this.isOnSent !== undefined && this._id) {
            // emulate normal off
            var offspec = this.host.newEventSpec(this.isOnSent ? 'off' : 'reoff');
            this.host.deliver(offspec, '', this);
        }
        this.host = null; // can't pass any more messages
    }
    if (this.katimer) {
        clearInterval(this.katimer);
        this.katimer = null;
    }
    if (this.stream) {
        try {
            this.stream.close();
        } catch (ex) {}
        this.stream = null;
    }
    this._id = null;
};

/**
 * Sends operation to remote
 */
Pipe.prototype.deliver = function pd(spec, val, src) {
    var self = this;
    val && val.constructor === Spec && (val = val.toString());
    if (spec.type() === 'Host') {
        switch (spec.op()) {
        case 'reoff':
            setTimeout(function itsOverReally() {
                self.isOnSent = undefined;
                self.close();
            }, 1);
            break;
        case 'off':
            setTimeout(function tickingBomb() {
                self.close();
            }, 5000);
            break;
        case 'on':
            this.isOnSent = true;
        case 'reon':
            this.isOnSent = false;
        }
    }
    this.bundle[spec] = val === undefined ? null : val; // TODO aggregation
    if (this.delay === -1) {
        this.sendBundle();
    } else if (!this.send_timer) {
        var now = this.time(),
            gap = now - this.lastSendTS,
            timeout = gap > this.delay ? this.delay : this.delay - gap;
        this.send_timer = setTimeout(this.sendBundle.bind(this), timeout); // hmmm...
    } // else {} // just wait
};

/** @returns {number} milliseconds as an int */
Pipe.prototype.time = function () { return new Date().getTime(); };

/**
 * @returns {Spec|string} remote host spec "/Host#peer_id" or empty string (when not handshaken yet)
 */
Pipe.prototype.spec = function () {
    return this._id ? new Spec('/Host#' + this._id) : '';
};
/**
 * @param {*} bundle is a bunch of operations in a form {operation_spec: operation_params_object}
 * @private
 */
Pipe.prototype.parseBundle = function pb(bundle) {
    var spec_list = [], spec, self = this;
    //parse specifiers
    for (spec in bundle) { spec && spec_list.push(new Spec(spec)); }
    spec_list.sort().reverse();
    while (spec = spec_list.pop()) {
        spec = Spec.as(spec);
        this.host.deliver(spec, bundle[spec], this);
        if (spec.type() === 'Host' && spec.op() === 'reoff') { //TODO check #id
            setTimeout(function () {
                self.isOnSent = undefined;
                self.close();
            }, 1);
        }
    }
};

var dotIn = new Spec('/Pipe.in');
var dotOut = new Spec('/Pipe.out');
var dotClose = new Spec('/Pipe.close');
//var dotOpen = new Spec('/Pipe.open');

/**
 * Sends operations buffered in this.bundle as a bundle {operation_spec: operation_params_object}
 * @private
 */
Pipe.prototype.sendBundle = function pS() {
    var payload = this.serializer.stringify(this.bundle);
    this.bundle = {};
    if (!this.stream) {
        this.send_timer = null;
        return; // too late
    }

    try {
        env.trace && env.log(dotOut, payload, this, this.host);
        this.stream.write(payload);
        this.lastSendTS = this.time();
    } catch (ex) {
        env.error('stream error on write: ' + ex, ex.stack);
        if (this._id) {
            this.close('stream error', ex);
        }
    } finally {
        this.send_timer = null;
    }
};

},{"./Spec":12,"./env":17}],9:[function(require,module,exports){
"use strict";

function ProxyListener() {
    console.log('ProxyListener')
    this.callbacks = null;
    this.owner = null;
}

ProxyListener.prototype.deliver = function (spec,value,src) {
    if (this.callbacks===null) { return; }
    var that = this.owner || src;
    for(var i=0; i<this.callbacks.length; i++) {
        var cb = this.callbacks[i];
        if (cb.constructor===Function) {
            cb.call(that,spec,value,src);
        } else {
            cb.deliver(spec,value,src);
        }
    }
};

ProxyListener.prototype.on = function (callback) {
    if (this.callbacks===null) { this.callbacks = []; }
    this.callbacks.push(callback);
};

ProxyListener.prototype.off = function (callback) {
    console.log('ProxyListener.prototype.off');
    if (this.callbacks===null) { return; }
    var i = this.callbacks.indexOf(callback);
    if (i!==-1) {
        this.callbacks.splice(i,1);
    } else {
        console.warn('listener unknown', callback);
    }
};

module.exports = ProxyListener;

},{}],10:[function(require,module,exports){
"use strict";

var Spec = require('./Spec');

/** Swarm is based on the Lamport model of time and events in a
  * distributed system, so Lamport timestamps are essential to
  * its functioning. In most of the cases, it is useful to
  * use actuall wall clock time to create timestamps. This
  * class creates second-precise Lamport timestamps.
  * Timestamp ordering is alphanumeric, length may vary.
  *
  * @param processId id of the process/clock to add to every
  *        timestamp (like !timeseq+gritzko~ssn, where gritzko
  *        is the user and ssn is a session id, so processId
  *        is "gritzko~ssn").
  * @param initTime normally, that is server-supplied timestamp
  *        to init our time offset; there is no guarantee about
  *        clock correctness on the client side
  */
var SecondPreciseClock = function (processId, timeOffsetMs) {
    if (!Spec.reTok.test(processId)) {
        throw new Error('invalid process id: '+processId);
    }
    this.id = processId;
    // sometimes we assume our local clock has some offset
    this.clockOffsetMs = 0;
    this.lastTimestamp = '';
    // although we try hard to use wall clock time, we must
    // obey Lamport logical clock rules, in particular our
    // timestamps must be greater than any other timestamps
    // previously seen
    this.lastTimeSeen = 0;
    this.lastSeqSeen = 0;
    if (timeOffsetMs) {
        this.clockOffsetMs = timeOffsetMs;
    }
};

var epochDate = new Date("Wed, 01 Jan 2014 00:00:00 GMT");
SecondPreciseClock.EPOCH = epochDate.getTime();

SecondPreciseClock.prototype.adjustTime = function (trueMs) {
    var localTime = this.ms();
    var clockOffsetMs = trueMs - localTime;
    this.clockOffsetMs = clockOffsetMs;
    var lastTS = this.lastTimeSeen;
    this.lastTimeSeen = 0;
    this.lastSeqSeen = 0;
    this.lastTimestamp = '';
    if ( this.seconds()+1 < lastTS ) {
        console.error("risky clock reset",this.lastTimestamp);
    }
};

SecondPreciseClock.prototype.ms = function () {
    var millis = new Date().getTime();
    millis -= SecondPreciseClock.EPOCH;
    return millis;
};

SecondPreciseClock.prototype.seconds = function () {
    var millis = this.ms();
    millis += this.clockOffsetMs;
    return (millis/1000) | 0;
};

SecondPreciseClock.prototype.issueTimestamp = function time () {
    var res = this.seconds();
    if (this.lastTimeSeen>res) { res = this.lastTimeSeen; }
    if (res>this.lastTimeSeen) { this.lastSeqSeen = -1; }
    this.lastTimeSeen = res;
    var seq = ++this.lastSeqSeen;
    if (seq>=(1<<12)) {throw new Error('max event freq is 4000Hz');}

    var baseTimeSeq = Spec.int2base(res, 5);
    if (seq>0) { baseTimeSeq+=Spec.int2base(seq, 2); }

    this.lastTimestamp = baseTimeSeq + '+' + this.id;
    return this.lastTimestamp;
};

//SecondPreciseClock.reQTokExt = new RegExp(Spec.rsTokExt); // no 'g'

SecondPreciseClock.prototype.parseTimestamp = function parse (ts) {
    var m = ts.match(Spec.reTokExt);
    if (!m) {throw new Error('malformed timestamp: '+ts);}
    var timeseq=m[1]; //, process=m[2];
    var time = timeseq.substr(0,5), seq = timeseq.substr(5);
    if (seq&&seq.length!==2) {
        throw new Error('malformed timestamp value: '+timeseq);
    }
    return {
        time: Spec.base2int(time),
        seq: seq ? Spec.base2int(seq) : 0
    };
};

/** Freshly issued Lamport logical tiemstamps must be greater than
    any timestamps previously seen. */
SecondPreciseClock.prototype.checkTimestamp = function see (ts) {
    if (ts<this.lastTimestamp) { return true; }
    var parsed = this.parseTimestamp(ts);
    if (parsed.time<this.lastTimeSeen) { return true; }
    var sec = this.seconds();
    if (parsed.time>sec+1) {
        return false; // back to the future
    }
    this.lastTimeSeen = parsed.time;
    this.lastSeqSeen = parsed.seq;
    return true;
};

SecondPreciseClock.prototype.timestamp2date = function (ts) {
    var parsed = this.parseTimestamp(ts);
    var millis = parsed.time * 1000 + SecondPreciseClock.EPOCH;
    return new Date(millis);
};


module.exports = SecondPreciseClock;

},{"./Spec":12}],11:[function(require,module,exports){
"use strict";

var env = require('./env');
var Spec = require('./Spec');
var Syncable = require('./Syncable');
var Model = require('./Model'); // TODO
var ProxyListener = require('./ProxyListener');
var CollectionMethodsMixin = require('./CollectionMethodsMixin');

/**
 * Backbone's Collection is essentially an array and arrays behave poorly
 * under concurrent writes (see OT). Hence, our primary collection type
 * is a {id:Model} Set. One may obtain a linearized version by sorting
 * them by keys or otherwise.
 * This basic Set implementation can only store objects of the same type.
 * @constructor
 */
module.exports = Syncable.extend('Set', {

    defaults: {
        _objects: Object,
        _oplog: Object,
        _proxy: ProxyListener
    },

    mixins: [
        CollectionMethodsMixin
    ],

    reactions: {
        init: function (spec,val,src) {
            this.forEach(function (obj) {
                obj.on(this._proxy);
            }, this);
        }
    },

    ops: {
        /**
         * Both Model and Set are oplog-only; they never pass the state on the wire,
         * only the oplog; new replicas are booted with distilled oplog as well.
         * So, this is the only point in the code that mutates the state of a Set.
         */
        change: function (spec, value, repl) {
            value = this.distillOp(spec, value);
            var key_spec;
            for (key_spec in value) {
                if (value[key_spec] === 1) {
                    if (!this._objects[key_spec]) { // only if object not in the set
                        this._objects[key_spec] = this._host.get(key_spec);
                        this._objects[key_spec].on(this._proxy);
                    }
                } else if (value[key_spec] === 0) {
                    if (this._objects[key_spec]) {
                        this._objects[key_spec].off(this._proxy);
                        delete this._objects[key_spec];
                    }
                } else {
                    env.log(this.spec(), 'unexpected val', JSON.stringify(value));
                }
            }
        }
    },

    validate: function (spec, val, src) {
        if (spec.op() !== 'change') {
            return '';
        }

        for (var key_spec in val) {
            // member spec validity
            if (Spec.pattern(key_spec) !== '/#') {
                return 'invalid spec: ' + key_spec;
            }
        }
        return '';
    },

    distillOp: function (spec, val) {
        if (spec.version() > this._version) {
            return val; // no concurrent op
        }
        var opkey = spec.filter('!.');
        this._oplog[opkey] = val;
        this.distillLog(); // may amend the value
        return this._oplog[opkey] || {};
    },

    distillLog: Model.prototype.distillLog,

    pojo: function () {
        // invoke super.pojo()
        var result = Syncable._pt.pojo.apply(this, arguments);
        result.entries = Object.keys(this._objects);
        return result;
    },

    /**
     * Adds an object to the set.
     * @param {Syncable} obj the object  //TODO , its id or its specifier.
     */
    addObject: function (obj) {
        var specs = {};
        specs[obj.spec()] = 1;
        this.change(specs);
    },
    // FIXME reactions to emit .add, .remove

    removeObject: function (obj) {
        var spec = obj._id ? obj.spec() : new Spec(obj).filter('/#');
        if (spec.pattern() !== '/#') {
            throw new Error('invalid spec: ' + spec);
        }
        var specs = {};
        specs[spec] = 0;
        this.change(specs);
    },

    /**
     * @param {Spec|string} key_spec key (specifier)
     * @returns {Syncable} object by key
     */
    get: function (key_spec) {
        key_spec = new Spec(key_spec).filter('/#');
        if (key_spec.pattern() !== '/#') {
            throw new Error("invalid spec");
        }
        return this._objects[key_spec];
    },

    /**
     * @param {function?} order
     * @returns {Array} sorted list of objects currently in set
     */
    list: function (order) {
        var ret = [];
        for (var key in this._objects) {
            ret.push(this._objects[key]);
        }
        ret.sort(order);
        return ret;
    },

    forEach: function (cb, thisArg) {
        var index = 0;
        for (var spec in this._objects) {
            cb.call(thisArg, this._objects[spec], index++);
        }
    },

    every: function (cb, thisArg) {
        var index = 0;
        for (var spec in this._objects) {
            if (!cb.call(thisArg, this._objects[spec], index++)) {
                return false;
            }
        }
        return true;
    },

    filter: function (cb, thisArg) {
        var res = [];
        this.forEach(function (entry, idx) {
            if (cb.call(thisArg, entry, idx)) {
                res.push(entry);
            }
        });
        return res;
    },

    map: function (cb, thisArg) {
        var res = [];
        this.forEach(function (entry, idx) {
            res.push(cb.call(thisArg, entry, idx));
        });
        return res;
    }
});

},{"./CollectionMethodsMixin":2,"./Model":7,"./ProxyListener":9,"./Spec":12,"./Syncable":14,"./env":17}],12:[function(require,module,exports){
"use strict";

//  S P E C I F I E R
//
//  The Swarm aims to switch fully from the classic HTTP
//  request-response client-server interaction pattern to continuous
//  real-time synchronization (WebSocket), possibly involving
//  client-to-client interaction (WebRTC) and client-side storage
//  (WebStorage). That demands (a) unification of transfer and storage
//  where possible and (b) transferring, processing and storing of
//  fine-grained changes.
//
//  That's why we use compound event identifiers named *specifiers*
//  instead of just regular "plain" object ids everyone is so used to.
//  Our ids have to fully describe the context of every small change as
//  it is likely to be delivered, processed and stored separately from
//  the rest of the related state.  For every atomic operation, be it a
//  field mutation or a method invocation, a specifier contains its
//  class, object id, a method name and, most importantly, its
//  version id.
//
//  A serialized specifier is a sequence of Base64 tokens each prefixed
//  with a "quant". A quant for a class name is '/', an object id is
//  prefixed with '#', a method with '.' and a version id with '!'.  A
//  special quant '+' separates parts of each token.  For example, a
//  typical version id looks like "!7AMTc+gritzko" which corresponds to
//  a version created on Tue Oct 22 2013 08:05:59 GMT by @gritzko (see
//  Host.time()).
//
//  A full serialized specifier looks like
//        /TodoItem#7AM0f+gritzko.done!7AMTc+gritzko
//  (a todo item created by @gritzko was marked 'done' by himself)
//
//  Specifiers are stored in strings, but we use a lightweight wrapper
//  class Spec to parse them easily. A wrapper is immutable as we pass
//  specifiers around a lot.

function Spec(str, quant) {
    if (str && str.constructor === Spec) {
        str = str.value;
    } else { // later we assume value has valid format
        str = (str || '').toString();
        if (quant && str.charAt(0) >= '0') {
            str = quant + str;
        }
        if (str.replace(Spec.reQTokExt, '')) {
            throw new Error('malformed specifier: ' + str);
        }
    }
    this.value = str;
    this.index = 0;
}
module.exports = Spec;

Spec.prototype.filter = function (quants) {
    var filterfn = //typeof(quants)==='function' ? quants :
                function (token, quant) {
                    return quants.indexOf(quant) !== -1 ? token : '';
                };
    return new Spec(this.value.replace(Spec.reQTokExt, filterfn));
};
Spec.pattern = function (spec) {
    return spec.toString().replace(Spec.reQTokExt, '$1');
};
Spec.prototype.isEmpty = function () {
    return this.value==='';
};
Spec.prototype.pattern = function () {
    return Spec.pattern(this.value);
};
Spec.prototype.token = function (quant) {
    var at = quant ? this.value.indexOf(quant, this.index) : this.index;
    if (at === -1) {
        return undefined;
    }
    Spec.reQTokExt.lastIndex = at;
    var m = Spec.reQTokExt.exec(this.value);
    this.index = Spec.reQTokExt.lastIndex;
    if (!m) {
        return undefined;
    }
    return {quant: m[1], body: m[2], bare: m[3], ext: m[4]};
};
Spec.prototype.get = function specGet(quant) {
    var i = this.value.indexOf(quant);
    if (i === -1) {
        return '';
    }
    Spec.reQTokExt.lastIndex = i;
    var m = Spec.reQTokExt.exec(this.value);
    return m && m[2];
};
Spec.prototype.tok = function specGet(quant) {
    var i = this.value.indexOf(quant);
    if (i === -1) { return ''; }
    Spec.reQTokExt.lastIndex = i;
    var m = Spec.reQTokExt.exec(this.value);
    return m && m[0];
};
Spec.prototype.has = function specHas(quant) {
    if (quant.length===1) {
        return this.value.indexOf(quant) !== -1;
    } else {
        var toks = this.value.match(Spec.reQTokExt);
        return toks.indexOf(quant) !== -1;
    }
};
Spec.prototype.set = function specSet(spec, quant) {
    var ret = new Spec(spec, quant);
    var m;
    Spec.reQTokExt.lastIndex = 0;
    while (null !== (m = Spec.reQTokExt.exec(this.value))) {
        if (!ret.has(m[1])) {
            ret = ret.add(m[0]);
        }
    }
    return ret.sort();
};
Spec.prototype.version = function () { return this.get('!'); };
Spec.prototype.op = function () { return this.get('.'); };
Spec.prototype.type = function () { return this.get('/'); };
Spec.prototype.id = function () { return this.get('#'); };
Spec.prototype.typeid = function () { return this.filter('/#'); };
Spec.prototype.source = function () { return this.token('!').ext; };

Spec.prototype.sort = function () {
    function Q(a, b) {
        var qa = a.charAt(0), qb = b.charAt(0), q = Spec.quants;
        return (q.indexOf(qa) - q.indexOf(qb)) || (a < b);
    }

    var split = this.value.match(Spec.reQTokExt);
    return new Spec(split ? split.sort(Q).join('') : '');
};

Spec.prototype.add = function (spec, quant) {
    if (spec.constructor !== Spec) {
        spec = new Spec(spec, quant);
    }
    return new Spec(this.value + spec.value);
};
Spec.prototype.toString = function () { return this.value; };


Spec.int2base = function (i, padlen) {
    if (i < 0 || i >= (1 << 30)) {
        throw new Error('out of range');
    }
    var ret = '', togo = padlen || 5;
    for (; i || (togo > 0); i >>= 6, togo--) {
        ret = Spec.base64.charAt(i & 63) + ret;
    }
    return ret;
};

Spec.prototype.fits = function (specFilter) {
    var myToks = this.value.match(Spec.reQTokExt);
    var filterToks = specFilter.match(Spec.reQTokExt), tok;
    while (tok=filterToks.pop()) {
        if (myToks.indexOf(tok) === -1) {
            return false;
        }
    }
    return true;
};

Spec.base2int = function (base) {
    var ret = 0, l = base.match(Spec.re64l);
    for (var shift = 0; l.length; shift += 6) {
        ret += Spec.base64.indexOf(l.pop()) << shift;
    }
    return ret;
};
Spec.parseToken = function (token_body) {
    Spec.reTokExt.lastIndex = -1;
    var m = Spec.reTokExt.exec(token_body);
    if (!m) {
        return null;
    }
    return {bare: m[1], ext: m[2] || 'swarm'}; // FIXME not generic
};

Spec.base64 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~';
Spec.rT = '[0-9A-Za-z_~]{1,80}'; // 60*8 bits is enough for everyone
Spec.reTok = new RegExp('^'+Spec.rT+'$'); // plain no-extension token
Spec.re64l = new RegExp('[0-9A-Za-z_~]', 'g');
Spec.quants = ['/', '#', '!', '.'];
Spec.rsTokExt = '^(=)(?:\\+(=))?$'.replace(/=/g, Spec.rT);
Spec.reTokExt = new RegExp(Spec.rsTokExt);
Spec.rsQTokExt = '([/#\\.!\\*])((=)(?:\\+(=))?)'.replace(/=/g, Spec.rT);
Spec.reQTokExt = new RegExp(Spec.rsQTokExt, 'g');
Spec.is = function (str) {
    if (str === null || str === undefined) {
        return false;
    }
    return str.constructor === Spec || '' === str.toString().replace(Spec.reQTokExt, '');
};
Spec.as = function (spec) {
    if (!spec) {
        return new Spec('');
    } else {
        return spec.constructor === Spec ? spec : new Spec(spec);
    }
};

Spec.Map = function VersionVectorAsAMap(vec) {
    this.map = {};
    if (vec) {
        this.add(vec);
    }
};
Spec.Map.prototype.add = function (versionVector) {
    var vec = new Spec(versionVector, '!'), tok;
    while (undefined !== (tok = vec.token('!'))) {
        var time = tok.bare, source = tok.ext || 'swarm';
        if (time > (this.map[source] || '')) {
            this.map[source] = time;
        }
    }
};
Spec.Map.prototype.covers = function (version) {
    Spec.reTokExt.lastIndex = 0;
    var m = Spec.reTokExt.exec(version);
    var ts = m[1], src = m[2] || 'swarm';
    return ts <= (this.map[src] || '');
};
Spec.Map.prototype.maxTs = function () {
    var ts = null,
        map = this.map;
    for (var src in map) {
        if (!ts || ts < map[src]) {
            ts = map[src];
        }
    }
    return ts;
};
Spec.Map.prototype.toString = function (trim) {
    trim = trim || {top: 10, rot: '0'};
    var top = trim.top || 10,
        rot = '!' + (trim.rot || '0'),
        ret = [],
        map = this.map;
    for (var src in map) {
        ret.push('!' + map[src] + (src === 'swarm' ? '' : '+' + src));
    }
    ret.sort().reverse();
    while (ret.length > top || ret[ret.length - 1] <= rot) {
        ret.pop();
    }
    return ret.join('') || '!0';
};

},{}],13:[function(require,module,exports){
"use strict";

var Syncable = require('./Syncable');

function Storage(async) {
    this.async = !!async || false;
    this.states = {};
    this.tails = {};
    this.counts = {};
    this._host = null;
    // many implementations do not push changes
    // so there are no listeners
    this.lstn = null;
    this._id = 'some_storage';
}
module.exports = Storage;
Storage.prototype.MAX_LOG_SIZE = 10;
Storage.prototype.isRoot = true; // may create global objects

Storage.prototype.deliver = function (spec, value, src) {
    var ret;
    switch (spec.op()) {
        // A storage is always an "uplink" so it never receives reon, reoff.
    case 'on':
        ret = this.on(spec, value, src); break;
    case 'off':
        ret = this.off(spec, value, src); break;
    case 'init':
        if (value._version) { // state
            ret = this.init(spec, value, src);
        } else { // patch
            var ti = spec.filter('/#');
            var specs = [], s;
            for(s in value._tail) {  specs.push(s);  }
            specs.sort();
            while (s=specs.pop()) {
                ret = this.anyOp( ti.add(s), value._tail[s], src);
            }
        }
        break;
    default:
        ret = this.anyOp(spec, value, src);
    }
    return ret;
};

Storage.prototype.on = function storageOn (spec, base, src) {
    var ti = spec.filter('/#');

    if (this.lstn) {
        var ls = this.lstn[ti];
        if (ls === undefined) {
            ls = src;
        } else if (ls !== src) {
            if (ls.constructor !== Array) {
                ls = [ls];
            }
            ls.push(src);
        }
        this.lstn[ti] = ls;
    }

    var self = this;
    var state;
    var tail;

    function sendResponse() {
        if (!state) {
            if (self.isRoot) {// && !spec.token('#').ext) {
                // make 0 state for a global object TODO move to Host
                state = {_version: '!0'};
            }
        }
        if (tail) {
            if (!state) {state={};}
            state._tail = state._tail || {};
            for (var s in tail) {
                state._tail[s] = tail[s];
            }
        }
        var tiv = ti.add(spec.version(), '!');
        if (state) {
            src.deliver(tiv.add('.init'), state, self);
            src.deliver(tiv.add('.reon'), Syncable.stateVersionVector(state), self); // TODO and the tail
        } else {
            src.deliver(tiv.add('.reon'), '!0', self); // state unknown
        }
    }

    this.readState(ti, function (err, s) {
        state = s || null;
        if (tail !== undefined) {
            sendResponse();
        }
    });

    this.readOps(ti, function (err, t) {
        tail = t || null;
        if (state !== undefined) {
            sendResponse();
        }
    });
};


Storage.prototype.off = function (spec, value, src) {
    if (!this.lstn) {
        return;
    }
    var ti = spec.filter('/#');
    var ls = this.lstn[ti];
    if (ls === src) {
        delete this.lstn[ti];
    } else if (ls && ls.constructor === Array) {
        var cleared = ls.filter(function (v) {return v !== src;});
        if (cleared.length) {
            this.lstn[ti] = cleared;
        } else {
            delete this.lstn[ti];
        }
    }
};

Storage.prototype.init = function (spec, state, src) {
    var ti = spec.filter('/#'), self=this;
    var saveops = this.tails[ti];
    this.writeState(spec, state, function (err) {
        if (err) {
            console.error('state dump error:', err);
        } else {
            var tail = self.tails[ti] || (self.tails[ti] = {});
            for(var op in saveops) { // OK, let's keep that in the log
                tail[op] = saveops[op];
            }
        }
    });
};


Storage.prototype.anyOp = function (spec, value, src) {
    var self = this;
    var ti = spec.filter('/#');
    this.writeOp(spec, value, function (err) {
        if (err) {
            this.close(err); // the log is sacred
        }
    });
    self.counts[ti] = self.counts[ti] || 0;
    if (++self.counts[ti]>self.MAX_LOG_SIZE) {
        // The storage piggybacks on the object's state/log handling logic
        // First, it adds an op to the log tail unless the log is too long...
        // ...otherwise it sends back a subscription effectively requesting
        // the state, on state arrival zeroes the tail.
        delete self.counts[ti];
        src.deliver(spec.set('.reon'), '!0.init', self);
    }
};


// In a real storage implementation, state and log often go into
// different backends, e.g. the state is saved to SQL/NoSQL db,
// while the log may live in a key-value storage.
// As long as the state has sufficient versioning info saved with
// it (like a version vector), we may purge the log lazily, once
// we are sure that the state is reliably saved. So, the log may
// overlap with the state (some ops are already applied). That
// provides some necessary resilience to workaround the lack of
// transactions across backends.
// In case third parties may write to the backend, go figure
// some way to deal with it (e.g. make a retrofit operation).
Storage.prototype.writeState = function (spec, state, cb) {
    var ti = spec.filter('/#');
    this.states[ti] = JSON.stringify(state);
    // tail is zeroed on state flush
    delete this.tails[ti];
    // callback is mandatory
    cb();
};

Storage.prototype.writeOp = function (spec, value, cb) {
    var ti = spec.filter('/#');
    var vm = spec.filter('!.');
    var tail = this.tails[ti] || (this.tails[ti] = {});
    if (tail[vm]) {
        console.error('op replay @storage'+vm+new Error().stack);
    }
    tail[vm] = JSON.stringify(value);
    cb();
};

Storage.prototype.readState = function (ti, callback) {
    var state = JSON.parse(this.states[ti] || null);

    function sendResponse() {
        callback(null, state);
    }

    // may force async behavior
    this.async ? setTimeout(sendResponse, 1) : sendResponse();
};

Storage.prototype.readOps = function (ti, callback) {
    var tail = JSON.parse(this.tails[ti] || null);
    callback(null, tail);
};

Storage.prototype.close = function (callback) {
    if (callback) { callback(); }
};

Storage.prototype.emit = function (spec,value) {
    var ti = spec.filter('/#');
    var ln = this.lstn[ti];
    if (!ln) {return;}
    if (ln && ln.constructor===Array) {
        for(var i=0; ln && i<ln.length; i++) {
            var l = ln[i];
            if (l && l.constructor===Function) {
                l(spec,value,this);
            } else if (l && l.deliver) {
                l.deliver(spec,value,this);
            }
        }
    } else if (ln && ln.deliver) {
        ln.deliver(spec,value,this);
    } else if (ln && ln.constructor===Function) {
        ln(spec,value,this);
    }
};

},{"./Syncable":14}],14:[function(require,module,exports){
"use strict";

var Spec = require('./Spec');
var env = require('./env');

/**
 * Syncable: an oplog-synchronized object
 * @constructor
 */
function Syncable() {
    // listeners represented as objects that have deliver() method
    this._lstn = [',']; // we unshift() uplink listeners and push() downlinks
    // ...so _lstn is like [server1, server2, storage, ',', view, listener]
    // The most correct way to specify a version is the version vector,
    // but that one may consume more space than the data itself in some cases.
    // Hence, _version is not a fully specified version vector (see version()
    // instead). _version is essentially is the greatest operation timestamp
    // (Lamport-like, i.e. "time+source"), sometimes amended with additional
    // timestamps. Its main features:
    // (1) changes once the object's state changes
    // (2) does it monotonically (in the alphanum order sense)
    this._version = '';
    // make sense of arguments
    var args = Array.prototype.slice.call(arguments);
    this._host = (args.length && args[args.length - 1]._type === 'Host') ?
            args.pop() : env.localhost;
    if (Spec.is(args[0])) {
        this._id = new Spec(args.shift()).id() || this._host.time();
    } else if (typeof(args[0]) === 'string') {
        this._id = args.shift(); // TODO format
    } else {
        this._id = this._host.time();
        this._version = '!0'; // may apply state in the constructor, see Model
    }
    //var state = args.length ? args.pop() : (fresh?{}:undefined);
    // register with the host
    var doubl = this._host.register(this);
    if (doubl !== this) { return doubl; }
    // locally created objects get state immediately
    // (while external-id objects need to query uplinks)
    /*if (fresh && state) {
     state._version = '!'+this._id;
     var pspec = this.spec().add(state._version).add('.init');
     this.deliver(pspec,state,this._host);
     }*/
    this.reset();
    // find uplinks, subscribe
    this.checkUplink();
    // TODO inplement state push
    return this;
}
module.exports = Syncable;

Syncable.types = {};
Syncable.isOpSink = function (obj) {
    if (!obj) { return false; }
    if (obj.constructor === Function) { return true; }
    if (obj.deliver && obj.deliver.constructor === Function) { return true; }
    return false;
};
Syncable.reMethodName = /^[a-z][a-z0-9]*([A-Z][a-z0-9]*)*$/;
Syncable.memberClasses = {ops:1,neutrals:1,remotes:1,defaults:1,reactions:1,mixins:1};
Syncable._default = {};

function fnname(fn) {
    if (fn.name) { return fn.name; }
    return fn.toString().match(/^function\s*([^\s(]+)/)[1];
}


/**
 * All CRDT model classes must extend syncable directly or indirectly. Syncable
 * provides all the necessary oplog- and state-related primitives and methods.
 * Every state-mutating method should be explicitly declared to be wrapped
 * by extend() (see 'ops', 'neutrals', 'remotes' sections in class declaration).
 * @param {function|string} fn
 * @param {{ops:object, neutrals:object, remotes:object}} own
 */
Syncable.extend = function (fn, own) {
    var parent = this, fnid;
    if (fn.constructor !== Function) {
        var id = fn.toString();
        fn = function SomeSyncable() {
            return parent.apply(this, arguments);
        };
        fnid = id; // if only it worked
    } else { // please call Syncable.constructor.apply(this,args) in your constructor
        fnid = fnname(fn);
    }

    // inheritance trick from backbone.js
    var SyncProto = function () {
        this.constructor = fn;
        this._neutrals = {};
        this._ops = {};
        this._reactions = {};

        var event,
            name;
        if (parent._pt) {
            //copy _neutrals & _ops from parent
            for (event in parent._pt._neutrals) {
                this._neutrals[event] = parent._pt._neutrals[event];
            }
            for (event in parent._pt._ops) {
                this._ops[event] = parent._pt._ops[event];
            }
        }

        // "Methods" are serialized, logged and delivered to replicas
        for (name in own.ops || {}) {
            if (Syncable.reMethodName.test(name)) {
                this._ops[name] = own.ops[name];
                this[name] = wrapCall(name);
            } else {
                console.warn('invalid op name:',name);
            }
        }

        // "Neutrals" don't change the state
        for (name in own.neutrals || {}) {
            if (Syncable.reMethodName.test(name)) {
                this._neutrals[name] = own.neutrals[name];
                this[name] = wrapCall(name);
            } else {
                console.warn('invalid neutral op name:',name);
            }
        }

        // "Remotes" are serialized and sent upstream (like RPC calls)
        for (name in own.remotes || {}) {
            if (Syncable.reMethodName.test(name)) {
                this[name] = wrapCall(name);
            } else {
                console.warn('invalid rpc name:',name);
            }
        }

        // add mixins
        (own.mixins || []).forEach(function (mixin) {
            for (var name in mixin) {
                this[name] = mixin[name];
            }
        }, this);

        // add other members
        for (name in own) {
            if (Syncable.reMethodName.test(name)) {
                var memberType = own[name].constructor;
                if (memberType === Function) { // non-op method
                    // these must change state ONLY by invoking ops
                    this[name] = own[name];
                } else if (memberType===String || memberType===Number) {
                    this[name] = own[name]; // some static constant, OK
                } else if (name in Syncable.memberClasses) {
                    // see above
                    continue;
                } else {
                    console.warn('invalid member:',name,memberType);
                }
            } else {
                console.warn('invalid member name:',name);
            }
        }

        // add reactions
        for (name in own.reactions || {}) {
            var reaction = own.reactions[name];
            if (!reaction) { continue; }

            switch (typeof reaction) {
            case 'function':
                // handler-function
                this._reactions[name] = [reaction];
                break;
            case 'string':
                // handler-method name
                this._reactions[name] = [this[name]];
                break;
            default:
                if (reaction.constructor === Array) {
                    // array of handlers
                    this._reactions[name] = reaction.map(function (item) {
                        switch (typeof item) {
                        case 'function':
                            return item;
                        case 'string':
                            return this[item];
                        default:
                            throw new Error('unexpected reaction type');
                        }
                    }, this);
                } else {
                    throw new Error('unexpected reaction type');
                }
            }
        }

        var syncProto = this;
        this.callReactions = function (spec, value, src) {
            var superReactions = syncProto._super.callReactions;
            if ('function' === typeof superReactions) {
                superReactions.call(this, spec, value, src);
            }
            var r = syncProto._reactions[spec.op()];
            if (r) {
                r.constructor !== Array && (r = [r]);
                for (var i = 0; i < r.length; i++) {
                    r[i] && r[i].call(this, spec, value, src);
                }
            }
        };

        this._super = parent.prototype;
        this._type = fnid;
    };

    SyncProto.prototype = parent.prototype;
    fn.prototype = new SyncProto();
    fn._pt = fn.prototype; // just a shortcut

    // default field values
    var key;
    var defs = fn.defaults = {};
    for (key in (parent.defaults || {})) {
        defs[key] = normalizeDefault(parent.defaults[key]);
    }
    for (key in (own.defaults || {})) {
        defs[key] = normalizeDefault(own.defaults[key]);
    }

    function normalizeDefault(val) {
        if (val && val.type) {
            return val;
        }
        if (val && val.constructor === Function) {
            return {type: val, value: undefined};
        }
        return {type:null, value: val};
    }

    // signature normalization for logged/remote/local method calls;
    function wrapCall(name) {
        return function wrapper() {
            // assign a Lamport timestamp
            var spec = this.newEventSpec(name);
            var args = Array.prototype.slice.apply(arguments), lstn;
            // find the callback if any
            Syncable.isOpSink(args[args.length - 1]) && (lstn = args.pop());
            // prettify the rest of the arguments
            if (!args.length) {  // FIXME isn't it confusing?
                args = ''; // used as 'empty'
            } else if (args.length === 1) {
                args = args[0]; // {key:val}
            }
            // TODO log 'initiated'
            return this.deliver(spec, args, lstn);
        };
    }

    // finishing touches
    fn._super = parent;
    fn.extend = this.extend;
    fn.addReaction = this.addReaction;
    fn.removeReaction = this.removeReaction;
    Syncable.types[fnid] = fn;
    return fn;
};

/**
 * A *reaction* is a hybrid of a listener and a method. It "reacts" on a
 * certain event for all objects of that type. The callback gets invoked
 * as a method, i.e. this===syncableObj. In an event-oriented architecture
 * reactions are rather handy, e.g. for creating mixins.
 * @param {string} op operation name
 * @param {function} fn callback
 * @returns {{op:string, fn:function}}
 */
Syncable.addReaction = function (op, fn) {
    var reactions = this.prototype._reactions;
    var list = reactions[op];
    list || (list = reactions[op] = []);
    list.push(fn);
    return {op: op, fn: fn};
};

/**
 *
 * @param handle
 */
Syncable.removeReaction = function (handle) {
    var op = handle.op,
        fn = handle.fn,
        list = this.prototype._reactions[op],
        i = list.indexOf(fn);
    if (i === -1) {
        throw new Error('reaction unknown');
    }
    list[i] = undefined; // such a peculiar pattern not to mess up out-of-callback removal
    while (list.length && !list[list.length - 1]) {
        list.pop();
    }
};

/**
 * compare two listeners
 * @param {{deliver:function, _src:*, sink:function}} ln listener from syncable._lstn
 * @param {function|{deliver:function}} other some other listener or function
 * @returns {boolean}
 */
Syncable.listenerEquals = function (ln, other) {
    return !!ln && ((ln === other) ||
        (ln._src && ln._src === other) ||
        (ln.fn && ln.fn === other) ||
        (ln.sink && ln.sink === other));
};

// Syncable includes all the oplog, change propagation and distributed
// garbage collection logix.
Syncable.extend(Syncable, {  // :P
    /**
     * @returns {Spec} specifier "/Type#objid"
     */
    spec: function () { return new Spec('/' + this._type + '#' + this._id); },

    /**
     * Generates new specifier with unique version
     * @param {string} op operation
     * @returns {Spec}
     */
    newEventSpec: function (op) {
        return this.spec().add(this._host.time(), '!').add(op, '.');
    },

    /**
     * Returns current object state specifier
     * @returns {string} specifier "/Type#objid!version+source[!version+source2...]"
     */
    stateSpec: function () {
        return this.spec() + (this._version || ''); //?
    },

    /**
     * Applies a serialized operation (or a batch thereof) to this replica
     */
    deliver: function (spec, value, lstn) {
        spec = Spec.as(spec);
        var opver = '!' + spec.version();
        var error;

        function fail(msg, ex) {
            console.error(msg, spec, value, (ex && ex.stack) || ex || new Error(msg));
            if (typeof(lstn) === 'function') {
                lstn(spec.set('.fail'), msg);
            } else if (lstn && typeof(lstn.error) === 'function') {
                lstn.error(spec, msg);
            } // else { } no callback provided
        }

        // sanity checks
        if (spec.pattern() !== '/#!.') {
            return fail('malformed spec', spec);
        }
        if (!this._id) {
            return fail('undead object invoked');
        }
        if (error = this.validate(spec, value)) {
            return fail('invalid input, ' + error, value);
        }
        if (!this.acl(spec, value, lstn)) {
            return fail('access violation', spec);
        }

        env.debug && env.log(spec, value, lstn);

        try {
            var call = spec.op();
            if (this._ops[call]) {  // FIXME name=>impl table
                if (this.isReplay(spec)) { // it happens
                    console.warn('replay', spec);
                    return;
                }
                // invoke the implementation
                this._ops[call].call(this, spec, value, lstn); // NOTE: no return value
                // once applied, may remember in the log...
                if (spec.op() !== 'init') {
                    this._oplog && (this._oplog[spec.filter('!.')] = value);
                    // this._version is practically a label that lets you know whether
                    // the state has changed. Also, it allows to detect some cases of
                    // concurrent change, as it is always set to the maximum version id
                    // received by this object. Still, only the full version vector may
                    // precisely and uniquely specify the current version (see version()).
                    this._version = (opver > this._version) ? opver : this._version + opver;
                } else {
                    value = this.diff('!0');
                }
                // ...and relay further to downstream replicas and various listeners
                this.emit(spec, value, lstn);
            } else if (this._neutrals[call]) {
                // invoke the implementation
                this._neutrals[call].call(this, spec, value, lstn);
                // and relay to listeners
                this.emit(spec, value, lstn);
            } else {
                this.unimplemented(spec, value, lstn);
            }
        } catch (ex) { // log and rethrow; don't relay further; don't log
            return fail("method execution failed", ex);
        }

        // to force async signatures we eat the returned value silently
        return spec;
    },

    /**
     * Notify all the listeners of a state change (i.e. the operation applied).
     */
    emit: function (spec, value, src) {
        var ls = this._lstn,
            op = spec.op(),
            is_neutrals = op in this._neutrals;
        if (ls) {
            var notify = [];
            for (var i = 0; i < ls.length; i++) {
                var l = ls[i];
                // skip empties, deferreds and the source
                if (!l || l === ',' || l === src) { continue; }
                if (is_neutrals && l._op !== op) { continue; }
                if (l._op && l._op !== op) { continue; }
                notify.push(l);
            }
            for (i = 0; i < notify.length; i++) { // screw it I want my 'this'
                try {
                    notify[i].deliver(spec, value, this);
                } catch (ex) {
                    console.error(ex.message, ex.stack);
                }
            }
        }
        this.callReactions(spec, value, src);
    },

    trigger: function (event, params) {
        var spec = this.newEventSpec(event);
        this.deliver(spec, params);
    },

    /**
     * Blindly applies a JSON changeset to this model.
     * @param {*} values
     */
    apply: function (values) {
        for (var key in values) {
            if (Syncable.reFieldName.test(key)) { // skip special fields
                var def = this.constructor.defaults[key];
                this[key] = def && def.type ?
                    new def.type(values[key]) : values[key];
            }
        }
    },

    /**
     * @returns {Spec.Map} the version vector for this object
     */
    version: function () {
        // distillLog() may drop some operations; still, those need to be counted
        // in the version vector; so, their Lamport ids must be saved in this._vector
        var map = new Spec.Map(this._version + (this._vector || ''));
        if (this._oplog) {
            for (var op in this._oplog) {
                map.add(op);
            }
        }
        return map; // TODO return the object, let the consumer trim it to taste
    },

    /**
     * Produce the entire state or probably the necessary difference
     * to synchronize a replica which is at version *base*.
     * The format of a state/patch object is:
     * {
     *   // A version label, see Syncable(). Presence of the label means
     *   // that this object has a snapshot of the state. No version
     *   // means it is a diff (log tail).
     *   _version: Spec,
     *   // Some parts of the version vector that can not be derived from
     *   // _oplog or _version.
     *   _vector: Spec,
     *   // Some ops that were already applied. See distillLog()
     *   _oplog: { spec: value },
     *   // Pending ops that need to be applied.
     *   _tail: { spec: value }
     * }
     *
     * The state object must survive JSON.parse(JSON.stringify(obj))
     *
     * In many cases, the size of a distilled log is small enough to
     * use it for state transfer (i.e. no snapshots needed).
     */
    diff: function (base) {
        //var vid = new Spec(this._version).get('!'); // first !token
        //var spec = vid + '.patch';
        if (!this._version) { return undefined; }
        this.distillLog(); // TODO optimize?
        var patch, spec;
        if (base && base != '!0' && base != '0') { // FIXME ugly
            var map = new Spec.Map(base || '');
            for (spec in this._oplog) {
                if (!map.covers(new Spec(spec).version())) {
                    patch = patch || {_tail: {}}; // NOTE: no _version
                    patch._tail[spec] = this._oplog[spec];
                }
            }
        } else {
            patch = {_version: '!0', _tail: {}}; // zero state plus the tail
            for (spec in this._oplog) {
                patch._tail[spec] = this._oplog[spec];
            }
        }
        return patch;
    },

    distillLog: function () {
    },

    /**
     * The method must decide whether the source of the operation has
     * the rights to perform it. The method may check both the nearest
     * source and the original author of the op.
     * If this method ever mentions 'this', that is a really bad sign.
     * @returns {boolean}
     */
    acl: function (spec, val, src) {
        return true;
    },

    /**
     * Check operation format/validity (recommendation: don't check against the current state)
     * @returns {string} '' if OK, error message otherwise.
     */
    validate: function (spec, val, src) {
        if (spec.pattern() !== '/#!.') {
            return 'incomplete event spec';
        }
        if (this.clock && spec.type()!=='Host' && !this.clock.checkTimestamp(spec.version())) {
            return 'invalid timestamp '+spec;
        }
    },

    /**
     * whether this op was already applied in the past
     * @returns {boolean}
     */
    isReplay: function (spec) {
        if (!this._version) { return false; }
        if (spec.op() === 'init') { return false; } // these are .on !vids
        var opver = spec.version();
        if (opver > this._version.substr(1)) { return false; }
        if (spec.filter('!.').toString() in this._oplog) { return true; }// TODO log trimming, vvectors?
        return this.version().covers(opver); // heavyweight
    },

    /**
     * External objects (those you create by supplying an id) need first to query
     * the uplink for their state. Before the state arrives they are stateless.
     * @return {boolean}
     */
    hasState: function () {
        return !!this._version;
    },

    getListenerIndex: function (search_for, uplinks_only) {
        var i = this._lstn.indexOf(search_for),
            l;
        if (i > -1) { return i; }

        for (i = 0, l = this._lstn.length; i < l; i++) {
            var ln = this._lstn[i];
            if (uplinks_only && ln === ',') {
                return -1;
            }
            if (Syncable.listenerEquals(ln, search_for)) {
                return i;
            }
        }
        return -1;
    },

    reset: function () {
        var defs = this.constructor.defaults;
        for (var name in defs) {
            var def = defs[name];
            if (def.type) {
                this[name] = def.value ? new def.type(def.value) : new def.type();
            } else {
                this[name] = def.value;
            }
        }
    },


    neutrals: {
        /**
         * Subscribe to the object's operations;
         * the upstream part of the two-way subscription
         *  on() with a full filter:
         *  @param {Spec} spec /Mouse#Mickey!now.on
         *  @param {Spec|string} filter !since.event
         *  @param {{deliver:function}|function} repl callback
         *  @this {Syncable}
         *
         * TODO: prevent second subscription
         */
        on: function (spec, filter, repl) {   // WELL  on() is not an op, right?
            // if no listener is supplied then the object is only
            // guaranteed to exist till the next Host.gc() run
            if (!repl) { return; }

            var self = this;
            // stateless objects fire no events; essentially, on() is deferred
            if (!this._version && filter) { // TODO solidify
                this._lstn.push({
                    _op: 'reon',
                    _src: repl,
                    deliver: function () {
                        var i = self._lstn.indexOf(this);
                        self._lstn.splice(i, 1);
                        self.deliver(spec, filter, repl);
                    }
                });
                return; // defer this call till uplinks are ready
            }
            // make all listeners uniform objects
            if (repl.constructor === Function) {
                repl = {
                    sink: repl,
                    that: this,
                    deliver: function () { // .deliver is invoked on an event
                        this.sink.apply(this.that, arguments);
                    }
                };
            }

            if (filter) {
                filter = new Spec(filter, '.');
                var baseVersion = filter.filter('!'),
                    filter_by_op = filter.get('.');

                if (filter_by_op === 'init') {
                    var diff_if_needed = baseVersion ? this.diff(baseVersion) : '';
                    repl.deliver(spec.set('.init'), diff_if_needed, this); //??
                    // FIXME use once()
                    return;
                }
                if (filter_by_op) {
                    repl = {
                        sink: repl,
                        _op: filter_by_op,
                        deliver: function deliverWithFilter(spec, val, src) {
                            if (spec.op() === filter_by_op) {
                                this.sink.deliver(spec, val, src);
                            }
                        }
                    };
                }

                if (!baseVersion.isEmpty()) {
                    var diff = this.diff(baseVersion);
                    diff && repl.deliver(spec.set('.init'), diff, this); // 2downlink
                    repl.deliver(spec.set('.reon'), this.version().toString(), this);
                }
            }

            this._lstn.push(repl);
            // TODO repeated subscriptions: send a diff, otherwise ignore
        },

        /**
         * downstream reciprocal subscription
         */
        reon: function (spec, filter, repl) {
            if (filter) {  // a diff is requested
                var base = Spec.as(filter).tok('!');
                var diff = this.diff(base);
                if (diff) {
                    repl.deliver(spec.set('.init'), diff, this);
                }
            }
        },

        /** Unsubscribe */
        off: function (spec, val, repl) {
            var idx = this.getListenerIndex(repl); //TODO ??? uplinks_only?
            if (idx > -1) {
                this._lstn.splice(idx, 1);
            }
        },

        /** Reciprocal unsubscription */
        reoff: function (spec, val, repl) {
            var idx = this.getListenerIndex(repl); //TODO ??? uplinks_only?
            if (idx > -1) {
                this._lstn.splice(idx, 1);
            }
            if (this._id) {
                this.checkUplink();
            }
        },

        /**
         * As all the event/operation processing is asynchronous, we
         * cannot simply throw/catch exceptions over the network.
         * This method allows to send errors back asynchronously.
         * Sort of an asynchronous complaint mailbox :)
         */
        error: function (spec, val, repl) {
            console.error('something failed:', spec, val, '@', (repl && repl._id));
        }

    }, // neutrals

    ops: {
        /**
         * A state of a Syncable CRDT object is transferred to a replica using
         * some combination of POJO state and oplog. For example, a simple LWW
         * object (Last Writer Wins, see Model.js) uses its distilled oplog
         * as the most concise form. A CT document (Causal Trees) has a highly
         * compressed state, its log being hundred times heavier. Hence, it
         * mainly uses its plain state, but sometimes its log tail as well. The
         * format of the state object is POJO plus (optionally) special fields:
         * _oplog, _tail, _vector, _version (the latter flags POJO presence).
         * In either case, .init is only produced by diff() (+ by storage).
         * Any real-time changes are transferred as individual events.
         * @this {Syncable}
         */
        init: function (spec, state, src) {

            var tail = {}, // ops to be applied on top of the received state
                typeid = spec.filter('/#'),
                lstn = this._lstn,
                a_spec;
            this._lstn = []; // prevent events from being fired

            if (state._version/* && state._version !== '!0'*/) {
                // local changes may need to be merged into the received state
                if (this._oplog) {
                    for (a_spec in this._oplog) {
                        tail[a_spec] = this._oplog[a_spec];
                    }
                    this._oplog = {};
                }
                this._vector && (this._vector = undefined);
                // zero everything
                for (var key in this) {
                    if (this.hasOwnProperty(key) && key.charAt(0) !== '_') {
                        this[key] = undefined;
                    }
                }
                // set default values
                this.reset();

                this.apply(state);
                this._version = state._version;

                state._oplog && (this._oplog = state._oplog); // FIXME copy
                state._vector && (this._vector = state._vector);
            }
            // add the received tail to the local one
            if (state._tail) {
                for (a_spec in state._tail) {
                    tail[a_spec] = state._tail[a_spec];
                }
            }
            // appply the combined tail to the new state
            var specs = [];
            for (a_spec in tail) {
                specs.push(a_spec);
            }
            specs.sort().reverse();
            // there will be some replays, but those will be ignored
            while (a_spec = specs.pop()) {
                this.deliver(typeid.add(a_spec), tail[a_spec], this);
            }

            this._lstn = lstn;

        }

    }, // ops


    /**
     * Uplink connections may be closed or reestablished so we need
     * to adjust every object's subscriptions time to time.
     * @this {Syncable}
     */
    checkUplink: function () {
        var new_uplinks = this._host.getSources(this.spec()).slice(),
            up, self = this;
        // the plan is to eliminate extra subscriptions and to
        // establish missing ones; that only affects outbound subs
        for (var i = 0; i < this._lstn.length && this._lstn[i] != ','; i++) {
            up = this._lstn[i];
            if (!up) {
                continue;
            }
            up._src && (up = up._src); // unready
            var up_idx = new_uplinks.indexOf(up);
            if (up_idx === -1) { // don't need this uplink anymore
                up.deliver(this.newEventSpec('off'), '', this);
            } else {
                new_uplinks[up_idx] = undefined;
            }
        }
        // subscribe to the new
        for (i = 0; i < new_uplinks.length; i++) {
            up = new_uplinks[i];
            if (!up) {
                continue;
            }
            var onspec = this.newEventSpec('on');
            this._lstn.unshift({
                _op: 'reon',
                _src: up,
                deliver: function (spec, base, src) {
                    if (spec.version() !== onspec.version()) {
                        return;
                    } // not mine

                    var i = self.getListenerIndex(this);
                    self._lstn[i] = up;
                }
            });
            up.deliver(onspec, this.version().toString(), this);
        }
    },

    /**
     * returns a Plain Javascript Object with the state
     * @this {Syncable}
     */
    pojo: function (addVersionInfo) {
        var pojo = {},
            defs = this.constructor.defaults;
        for (var key in this) {
            if (this.hasOwnProperty(key)) {
                if (Syncable.reFieldName.test(key) && this[key] !== undefined) {
                    var def = defs[key],
                        val = this[key];
                    pojo[key] = def && def.type ?
                    (val.toJSON && val.toJSON()) || val.toString() :
                            (val && val._id ? val._id : val); // TODO prettify
                }
            }
        }
        if (addVersionInfo) {
            pojo._id = this._id; // not necassary
            pojo._version = this._version;
            this._vector && (pojo._vector = this._vector);
            this._oplog && (pojo._oplog = this._oplog); //TODO copy
        }
        return pojo;
    },

    /**
     * Sometimes we get an operation we don't support; not normally
     * happens for a regular replica, but still needs to be caught
     */
    unimplemented: function (spec, val, repl) {
        console.warn("method not implemented:", spec);
    },

    /**
     * Deallocate everything, free all resources.
     */
    close: function () {
        var l = this._lstn,
            s = this.spec(),
            uplink;

        this._id = null; // no id - no object; prevent relinking
        while ((uplink = l.shift()) && uplink !== ',') {
            uplink.off(s, null, this);
        }
        while (l.length) {
            l.pop().deliver(s.set('.reoff'), null, this);
        }
        this._host.unregister(this);
    },

    /**
     * Once an object is not listened by anyone it is perfectly safe
     * to garbage collect it.
     */
    gc: function () {
        var l = this._lstn;
        if (!l.length || (l.length === 1 && !l[0])) {
            this.close();
        }
    },

    /**
     * @param {string} filter event filter for subscription
     * @param {function} cb callback (will be called once)
     * @see Syncable#on
     */
    once: function (filter, cb) {
        this.on(filter, function onceWrap(spec, val, src) {
            // "this" is the object (Syncable)
            if (cb.constructor === Function) {
                cb.call(this, spec, val, src);
            } else {
                cb.deliver(spec, val, src);
            }
            this.off(filter, onceWrap);
        });
    }
});


Syncable.reFieldName = /^[a-z][a-z0-9]*([A-Z][a-z0-9]*)*$/;

/**
 * Derive version vector from a state of a Syncable object.
 * This is not a method as it needs to be applied to a flat JSON object.
 * @see Syncable.version
 * @see Spec.Map
 * @returns {string} string representation of Spec.Map
 */
Syncable.stateVersionVector = function stateVersionVector(state) {
    var op,
        map = new Spec.Map( (state._version||'!0') + (state._vector || '') );
    if (state._oplog) {
        for (op in state._oplog) {
            map.add(op);
        }
    }
    if (state._tail) {
        for (op in state._tail) {
            map.add(op);
        }
    }
    return map.toString();
};

},{"./Spec":12,"./env":17}],15:[function(require,module,exports){
"use strict";

var Spec = require('./Spec');
var Syncable = require('./Syncable');

var Text = Syncable.extend('Text', {
    // naive uncompressed CT weave implementation
    defaults: {
        weave: '\n',
        ids: {type:Array, value:'00000+swarm'},
        text: '',
        _oplog: Object
    },

    neutrals: {
        state: function (spec, text, src) {
            console.log('what?');
        }
    },
    ops: {
        insert: function (spec, ins, src) {
            var w1 = [], w4 = [];
            var vt = spec.token('!'), v = vt.bare;
            var ts = v.substr(0, 5), seq = v.substr(5) || '00';
            var seqi = Spec.base2int(seq);
            for (var i = 0; i < this.weave.length; i++) {
                var id = this.ids[i];
                w1.push(this.weave.charAt(i));
                w4.push(id);
                if (id in ins) {
                    var str = ins[id].toString();
                    var k = i + 1;
                    while (k < this.weave.length && this.ids[k] > vt.body) {
                        k++;
                    }
                    if (k > i + 1) { // concurrent edits
                        var newid = this.ids[k - 1];
                        ins[newid] = ins[id];
                        delete ins[id];
                    } else {
                        for (var j = 0; j < str.length; j++) {
                            w1.push(str.charAt(j)); // FIXME overfill
                            var genTs = ts + (seqi ? Spec.int2base(seqi++, 2) : '') + '+' + vt.ext;
                            w4.push(genTs);
                            if (!seqi) {
                                seqi = 1; // FIXME repeat ids, double insert
                            }
                        }
                    }
                }
            }
            if (genTs) {
                this._host.clock.checkTimestamp(genTs);
            }
            this.weave = w1.join('');
            this.ids = w4;
            this.rebuild();
        },
        remove: function (spec, rm, src) {
            var w1 = [], w4 = [];
            var v = spec.version();
            for (var i = 0; i < this.weave.length; i++) {
                w1.push(this.weave.charAt(i));
                w4.push(this.ids[i]);
                if (this.ids[i] in rm) {
                    w1.push('\u0008');
                    w4.push(v);
                }
            }
            this.weave = w1.join('');
            this.ids = w4;
            this.rebuild();
        }
    },
    rebuild: function () {
        /*var re = /([^\u0008][\u0008]+)|([^\u0008])/g, m=[];
         var text = [], tids = [], pos = 0;
         while (m=re.exec(this.weave)) {
         if (m[2]) {
         text.push(m[2]);
         tids.push(this.ids[pos]);
         }
         pos += m[0].length;
         }

         this.tids = tids;*/
        this.text = this.weave.replace(/[^\u0008][\u0008]+/mg, '').substr(1);
    },
    set: function (newText) {
        var patch = Text.diff(this.text, newText);
        var rm = null, ins = null, weave = this.weave;
        var re_atom = /[^\u0008]([^\u0008][\u0008]+)*/mg;
        var atom;

        function skip(n) {
            for (n = n || 1; n > 0; n--) {
                atom = re_atom.exec(weave);
            }
        }

        skip(1); // \n #00000+swarm

        for (var i = 0; i < patch.length; i++) {
            var op = patch[i][0], val = patch[i][1];
            switch (op) {
            case '+':
                ins || (ins = {});
                ins[this.ids[atom.index]] = val;
                break;
            case '-':
                rm || (rm = {});
                for (var r = 0; r < val.length; r++) {
                    rm[this.ids[atom.index + atom[0].length]] = true;
                    skip();
                }
                break;
            case '=':
                skip(val.length);
            }
        }
        rm && this.remove(rm);
        ins && this.insert(ins);
    }
});

Text.diff = function diff(was, is) {
    var ret = [];
    // prefix suffix the rest is change
    var pre = 0;
    while (pre < was.length && pre < is.length && was.charAt(pre) === is.charAt(pre)) {
        pre++;
    }
    var post = 0;
    while (post < was.length - pre && post < is.length - pre &&
    was.charAt(was.length - post - 1) === is.charAt(is.length - post - 1)) {
        post++;
    }
    if (pre) {
        ret.push(['=', was.substr(0, pre)]);
    }
    var ins = is.length - pre - post;
    if (ins) {
        ret.push(['+', is.substr(pre, ins)]);
    }
    var rm = was.length - pre - post;
    if (rm) {
        ret.push(['-', was.substr(pre, rm)]);
    }
    if (post) {
        ret.push(['=', was.substr(pre + rm)]);
    }
    return ret;

};

module.exports = Text;

},{"./Spec":12,"./Syncable":14}],16:[function(require,module,exports){
"use strict";

var Spec = require('./Spec');
var LongSpec = require('./LongSpec');
var Syncable = require('./Syncable');
var ProxyListener = require('./ProxyListener');
var CollectionMethodsMixin = require('./CollectionMethodsMixin');

/** In distributed environments, linear structures are tricky. It is always
 *  recommended to use (sorted) Set as your default collection type. Still, in
 *  some cases we need precisely a Vector, so here it is. Note that a vector can
 *  not prune its mutation history for quite a while, so it is better not to
 *  sort (reorder) it repeatedly. The perfect usage pattern is a growing vector+
 *  insert sort or no sort at all. If you need to re-shuffle a vector
 *  differently or replace its contents, you'd better create a new vector.
 *  So, you've been warned.
 *  Vector is implemented on top of a LongSpec, so the API is very much alike.
 *  The replication/convergence/correctness algorithm is Causal Trees.
 *
 *  TODO support JSON types (as a part of ref-gen-refac)
 */
module.exports = Syncable.extend('Vector', {

    defaults: {
        _oplog: Object,
        _objects: Array,
        _order: LongSpec,
        _proxy: ProxyListener
    },

    mixins: [
        CollectionMethodsMixin
    ],

    ops: {  // operations is our assembly language

        // insert an object
        in: function (spec, value, src) {
            // we misuse specifiers to express the operation in
            // a compact non-ambiguous way
            value = new Spec(value);
            var opid = spec.tok('!');
            var at = value.tok('!');
            if (opid<=at) {
                throw new Error('timestamps are messed up');
            }
            var what = value.tok('#');
            if (!what) { throw new Error('object #id not specified'); }
            var type = value.get('/');
            if (!type && this.objectType) {
                type = this.objectType.prototype._type;
            }
            if (!type) {
                throw new Error('object /type not specified');
            }
            type = '/' + type;

            var pos = this.findPositionFor(opid, at?at:'!0');
            var obj = this._host.get(type+what);

            this._objects.splice(pos.index,0,obj);
            this._order.insert(opid,pos);

            obj.on(this._proxy);
        },

        // remove an object
        rm: function (spec, value, src) {
            value = Spec.as(value);
            var target = value.tok('!');
            var hint = value.has('.') ? Spec.base2int(value.get('.')) : 0;
            var at = this._order.find(target, Math.max(0,hint-5));
            if (at.end()) {
                at = this._order.find(target, 0);
            }
            if (at.end()) {
                // this can only be explained by concurrent deletion
                // partial order can't break cause-and-effect ordering
                return;
            }
            var obj = this._objects[at.index];
            this._objects.splice(at.index,1);
            at.erase(1);

            obj.off(this._proxy);
        }

        /** Either thombstones or log  before HORIZON
        patch: function (spec, value, src) {

        }*/

    },

    distillLog: function () {
        // TODO HORIZON
    },

    reactions: {

        'init': function fillAll (spec,val,src) { // TODO: reactions, state init tests
            for(var i=this._order.iterator(); !i.end(); i.next()) {
                var op = i.token() + '.in';
                var value = this._oplog[op];
                var obj = this.getObject(value);
                this._objects[i.index] = obj;
                obj.on(this._proxy);
            }
        }

    },

    pojo: function () {
        // invoke super.pojo()
        var result = Syncable._pt.pojo.apply(this, arguments);
        result.entries = Object.keys(this._objects);
        return result;
    },

    getObject: function (spec) {
        spec = new Spec(spec,'#');
        if (!spec.has('/')) {
            if (this.objectType) {
                spec = spec.add(this.objectType.prototype._type,'/').sort();
            } else {
                throw new Error("type not specified"); // TODO is it necessary at all?
            }
        }
        var obj = this._host.get(spec);
        return obj;
    },

    length: function () {
        return this._objects.length;
    },

    //  C A U S A L  T R E E S  M A G I C

    findPositionFor: function (id, parentId) { // FIXME protected methods && statics (entryType)
        if (!parentId) {
            parentId = this.getParentOf(id);
        }
        var next;
        if (parentId!=='!0') {
            next = this._order.find(parentId);
            if (next.end()) {
                next = this.findPositionFor(parentId);
            }
            next.next();
        } else {
            next = this._order.iterator();
        }
        // skip "younger" concurrent siblings
        while (!next.end()) {
            var nextId = next.token();
            if (nextId<id) {
                break;
            }
            var subtreeId = this.inSubtreeOf(nextId,parentId);
            if (!subtreeId || subtreeId<id) {
                break;
            }
            this.skipSubtree(next,subtreeId);
        }
        return next; // insert before
    },

    getParentOf: function (id) {
        var spec = this._oplog[id+'.in'];
        if (!spec) {
            throw new Error('operation unknown: '+id);
        }
        var parentId = Spec.as(spec).tok('!') || '!0';
        return parentId;
    },

    /** returns the immediate child of the root node that is an ancestor
      * of the given node. */
    inSubtreeOf: function (nodeId, rootId) {
        var id=nodeId, p=id;
        while (id>rootId) {
            p=id;
            id=this.getParentOf(id);
        }
        return id===rootId && p;
    },

    isDescendantOf: function (nodeId, rootId) {
        var i=nodeId;
        while (i>rootId) {
            i=this.getParentOf(i);
        }
        return i===rootId;
    },

    skipSubtree: function (iter, root) {
        root = root || iter.token();
        do {
            iter.next();
        } while (!iter.end() && this.isDescendantOf(iter.token(),root));
        return iter;
    },

    validate: function (spec, val, source) {
        // ref op is known
    },

    //  A R R A Y - L I K E  A P I
    //  wrapper methods that convert into op calls above

    indexOf: function (obj, startAt) {
        if (!obj._id) {
            obj = this.getObject(obj);
        }
        return this._objects.indexOf(obj,startAt);
    },

    /*splice: function (offset, removeCount, insert) {
        var ref = offset===-1 ? '' : this._objects[offset];
        var del = [];
        var hint;
        for (var rm=1; rm<=removeCount; rm++) {
            del.push(this._order.entryAt(offset+rm));
        }
        for(var a=3; a<this.arguments.length; a++) {
            var arg = this.arguments[a];
            arg = _id in arg ? arg._id : arg;
            if (!Spec.isId(arg)) { throw new Error('malformed id: '+arg); }
            ins.push(arg);
        }
        while (rmid=del.pop()) {
            this.del(rmid+hint);
        }
        while (insid=ins.pop()) {
            this.ins(ref+insid+hint);
        }
    },*/

    normalizePos: function (pos) {
        if (pos && pos._id) {
            pos=pos._id;
        }
        var spec = new Spec(pos,'#');
        var type = spec.type();
        var id = spec.id();
        for(var i=0; i<this._objects.length; i++) {
            var obj = this._objects[i];
            if (obj && obj._id===id && (!type || obj._type===type)) {
                break;
            }
        }
        return i;
    },

    /** Assuming position 0 on the "left" and left-to-right writing, the
      * logic of causal tree insertion is
      * insert(newEntry, parentWhichIsOnTheLeftSide). */
    insert: function (spec, pos) {
        // TODO bulk insert: make'em siblings
        if (pos===undefined) {
            pos = -1; // TODO ? this._order.length()
        }
        if (pos.constructor!==Number) {
            pos = this.normalizePos(pos);
        }
        if (spec && spec._id) {
            spec = spec.spec();
        } else /*if (spec.constructor===String)*/ {
            spec = new Spec(spec,'#');
        }
        // TODO new object
        var opid = pos===-1 ? '!0' : this._order.tokenAt(pos);
        // TODO hint pos
        return this.in(spec+opid);
    },

    insertAfter: function (obj, pos) {
        this.insert (obj,pos);
    },

    insertBefore: function (spec, pos) {
        if (pos===undefined) {
            pos = this._order.length();
        }
        if (pos.constructor!==Number) {
            pos = this.normalizePos(pos);
        }
        this.insert(spec,pos-1);
    },

    append: function append (spec) {
        this.insert(spec,this._order.length()-1);
    },

    remove: function remove (pos) {
        if (pos.constructor!==Number) {
            pos = this.normalizePos(pos);
        }
        var hint = Spec.int2base(pos,0);
        var op = this._order.tokenAt(pos);
        this.rm(op+'.'+hint); // TODO generic spec quants
    },

    // Set-compatible, in a sense
    addObject: function (obj) {
        this.append(obj);
    },

    removeObject: function (pos) {
        this.remove(pos);
    },

    objectAt: function (i) {
        return this._objects[i];
    },

    insertSorted: function (obj, cmp) {
    },

    setOrder: function (fn) {
    },

    forEach: function (cb, thisArg) {
        this._objects.forEach(cb, thisArg);
    },

    every: function (cb, thisArg) {
        return this._objects.every(cb, thisArg);
    },

    filter: function (cb, thisArg) {
        return this._objects.filter(cb, thisArg);
    },

    map: function (cb, thisArg) {
        return this._objects.map(cb, thisArg);
    }

});

},{"./CollectionMethodsMixin":2,"./LongSpec":5,"./ProxyListener":9,"./Spec":12,"./Syncable":14}],17:[function(require,module,exports){
"use strict";

/** a really simplistic default hash function */
function djb2Hash(str) {
    var hash = 5381;
    for (var i = 0; i < str.length; i++) {
        hash = ((hash << 5) + hash) + str.charCodeAt(i);
    }
    return hash;
}

var env = module.exports = {
    // maps URI schemes to stream implementations
    streams: {},
    // the default host
    localhost: undefined,
    // whether multiple hosts are allowed in one process
    // (that is mostly useful for testing)
    multihost: false,
    // hash function used for consistent hashing
    hashfn: djb2Hash,

    log: plain_log,
    debug: false,
    trace: false,

    isServer: typeof(navigator) === 'undefined',
    isBrowser: typeof(navigator) === 'object',
    isWebKit: false,
    isGecko: false,
    isIE: false,
    clockType: undefined // default
};

if (typeof(navigator) === 'object') {
    var agent = navigator.userAgent;
    env.isWebKit = /AppleWebKit\/(\S+)/.test(agent);
    env.isIE = /MSIE ([^;]+)/.test(agent);
    env.isGecko = /rv:.* Gecko\/\d{8}/.test(agent);
}

function plain_log(spec, val, object) {
    var method = 'log';
    switch (spec.op()) {
    case 'error':
        method = 'error';
        break;
    case 'warn':
        method = 'warn';
        break;
    }
    console[method](spec.toString(), val, object && object._id,
            '@' + ((object && object._host && object._host._id) || ''));
}

},{}],18:[function(require,module,exports){
"use strict";
var env = require('../lib/env');
var Spec = require('../lib/Spec');
var SecondPreciseClock = require('../lib/SecondPreciseClock');
var MinutePreciseClock = require('../lib/MinutePreciseClock');
var LamportClock = require('../lib/LamportClock');

env.debug = console.log;
env.multihost = true;

asyncTest('1.a timestamp sequence test', function () {
    var clock = new SecondPreciseClock('gritzko');
    expect(100);
    var ts1 = clock.issueTimestamp(), ts2, i=0;
    var iv = setInterval(function(){
        ts2 = clock.issueTimestamp();
        if (ts2<=ts1) {
            console.error(ts2, '<=', ts1);
        }
        if (i++==100) {
            start();
            clearInterval(iv);
        } else {
            ok(ts2 > ts1);
        }
        ts1 = ts2;
    }, 0);
    //swarm.close();
});

test('1.b basic specifier syntax', function (test) {
    var testSpec = '/Class#ID!7Umum+gritzko.event';
    var spec = new Spec(testSpec);
    equal(spec.version(),'7Umum+gritzko');
    equal(spec.token('!').ext,'gritzko');
    var rev = spec.toString();
    equal(rev,testSpec);
    /*var time = '20130811192020';
    var iso = Spec.timestamp2iso(time);
    var date = new Date(iso);
    test.equal(date.getMonth(),7); // zero based
    test.equal(date.getSeconds(),20);*/
    var spec2 = new Spec(spec);
    equal(spec.toString(),spec2.toString());
    var def = new Spec('/Type#id!ver.method');
    var over = def.set('#newid.newmethod');
    equal(over,'/Type#newid!ver.newmethod');
    var abc = new Spec('!abc');
    equal(abc.has('!ab'), false); // ?
    equal(abc.has('!'), true);
});

test('1.c spec filters', function () {
    var filter = '.on';
    equal (new Spec('!abc.on/Class').fits(filter), true);
    equal (new Spec('.off/Class').fits(filter), false);
    equal (new Spec('/Type!abc.off.on').fits(filter), true);

});

test('1.d version vector', function (){
    // the convention is: use "!version" for vectors and
    // simply "version" for scalars
    var vec = '!7AM0f+gritzko!0longago+krdkv!7AMTc+aleksisha!0ld!00ld#some+garbage';
    var map = new Spec.Map(vec);
    ok(map.covers('7AM0f+gritzko'));
    ok(!map.covers('7AMTd+aleksisha'));
    ok(!map.covers('6AMTd+maxmaxmax'));
    ok(map.covers('0ld'));
    ok(!map.covers('0le'));
    equal(map.map['swarm'],'0ld');
    ok(!('garbage' in map.map));
    equal(map.toString({rot:'6'}),'!7AMTc+aleksisha!7AM0f+gritzko');
    equal(map.toString({rot:'6',top:1}),'!7AMTc+aleksisha');

    var map2 = new Spec.Map("!1QDpv03+anon000qO!1P7AE05+anon000Bu");
    equal(!map2.covers(new Spec('!1P7AE05+anon000Bu.in').version()),false);
});

test('1.e corner cases', function () {
    var empty = new Spec('');
    equal(empty.type()||empty.id()||empty.op()||empty.version(),'');
    equal(empty.toString(),'');
    var action = new Spec('.on+re');
    equal(action.op(),'on+re');
    var fieldSet = new Spec('/TodoItem#7AM0f+gritzko!7AMTc+gritzko.set');
    equal(fieldSet.type(),'TodoItem');
    equal(fieldSet.id(),'7AM0f+gritzko');
    equal(fieldSet.version(),'7AMTc+gritzko');
    equal(fieldSet.op(),'set');
});

test('1.f minute-precise clox', function(test){
    var clock = new MinutePreciseClock('min');
    var prevts = '';
    for(var i=0; i<64; i++) {
        var ts = clock.issueTimestamp();
        ok(/^[0-9a-zA-Z_~]{5}\+min$/.test(ts));
        ok(ts>prevts);
        prevts = ts;
    }
    for(var i=0; i<130; i++) {
        var ts = clock.issueTimestamp();
    }
    ok(/^[0-9a-zA-Z_~]{7}\+min$/.test(ts));

    // tick 60 times
    // check the last char is changing
    // unless minute changed then restart
    // tick 60 times
    // see it spills over (extended ts)
});

test('1.g timestamp-ahead', function(test){
    var clock = new SecondPreciseClock('normal');
    var ts = clock.issueTimestamp();
    var parsed = clock.parseTimestamp(ts);
    //var tenAhead = Spec.int2base(parsed.time+10, 5)+'+ahead';
    //var tenBehind = Spec.int2base(parsed.time-10, 5)+'+behind';
    var clockAhead = new SecondPreciseClock('ahead', 10000);
    var clockBehind = new SecondPreciseClock('behind', -10000);
    var tsAhead = clockAhead.issueTimestamp();
    var tsBehind = clockBehind.issueTimestamp();
    ok(tsAhead>ts);
    ok(ts>tsBehind);
});

test('1.h timestamp to date', function(test){
    var clock = new SecondPreciseClock('normal');
    var ts = clock.issueTimestamp();
    var date = new Date();
    var recovered = clock.timestamp2date(ts);
    ok( Math.abs(date.getTime()-recovered.getTime()) < 2000 );
});

test('1.i Lamport clocks', function(test){
    var clock = new LamportClock('leslie');
    var ts1 = clock.issueTimestamp();
    equal(ts1,'00000+leslie');
    var ts2 = clock.issueTimestamp();
    equal(ts2,'00001+leslie');
    clock.checkTimestamp('00004+leslie');
    equal(clock.issueTimestamp(),'00005+leslie');
});

//var Empty = Swarm.Syncable.extend('Empty',{});

/*

    Vintage Victorian tests. Cool.

test('dry handshake', function () {
    var v = 0;
    var host = {
        _id: 'DummyHost',
        version: function () {
            return Spec.int2base(++v);
        },
        register: function (obj) {
            return obj;
        },
        availableUplinks: function (spec) {
            return spec.toString().indexOf('down')!==-1?[up]:[this];
        },
        on: function (stub,stub2,caller) {
            caller.reon(stub,'',this);
        },
        constructor: Host
    };
    var up = new Empty('up',{},host);
    var down = new Empty('down',{},host);
    // up.on('','!0',down);
    equal(up._lstn[0],host);
    equal(up._lstn[1],down);
    equal(down._lstn[0],up);
});*/

/*exports.testBase = function (test) {
    var obj = {
        '_vid': {
            text:   '!20130811192020&gritzko+iO',
            number: '!20130811192021&gritzko+iO',
            obj:    '!20130811192021+222&aleksisha',
            smth:   '!2013081019202+999&oldmf'
        },
        text: 'test',
        number: 123,
        obj: {},
        smth: 1
    };
    var base = Spec.getBase(obj);
    test.deepEqual(base,{
            '_':'20130811182021',
            'gritzko+iO':'20130811192021',
            'aleksisha':'20130811192021+222'
    });
    obj.smth = 4;
    var nts = '!20130811192028&gritzko+iO';
    obj['_vid'].smth = nts;
    var diff = Spec.getDiff(base,obj);
    test.equal(diff.smth,4);
    test.equal(diff['_vid'].smth, nts);
    test.done();
}*/

},{"../lib/LamportClock":4,"../lib/MinutePreciseClock":6,"../lib/SecondPreciseClock":10,"../lib/Spec":12,"../lib/env":17}],19:[function(require,module,exports){
"use strict";

var env = require('../lib/env');
var Spec = require('../lib/Spec');
var Host = require('../lib/Host');
var Model = require('../lib/Model');
var SyncSet = require('../lib/Set');
var Storage = require('../lib/Storage');

env.multihost = true;
env.debug = console.log;

MetricLengthField.metricRe = /(\d+)(mm|cm|m|km)?/g;  // "1m and 10cm"
MetricLengthField.scale = { m:1, cm:0.01, mm:0.001, km:1000 };
MetricLengthField.scaleArray = ['km','m','cm','mm'];

function MetricLengthField (value) {
    // convert mm cm m km
    if (typeof(value)==='number') {
        this.meters = value;
    } else {
        value = value.toString();
        this.meters=0;
        var m=[], scale=MetricLengthField.scale;
        MetricLengthField.metricRe.lastIndex = 0;
        while (m=MetricLengthField.metricRe.exec(value)) {
            var unit = m[2] ? scale[m[2]] : 1;
            this.meters += parseInt(m[1]) * unit;
        }
    }
}
MetricLengthField.prototype.add = function () {

};
// .pojo() invokes (entry.toJSON&&entry.toJSON()) || entry.toString()
MetricLengthField.prototype.toString = function () {
    var m = this.meters, ret='', scar = MetricLengthField.scaleArray;
    for(var i=0; i<scar.length; i++) {
        var unit = scar[i],
            scale= MetricLengthField.scale[unit];
        var wholeUnits = Math.floor(m/scale);
        if (wholeUnits>=1) {
            ret += wholeUnits + unit;
        }
        m -= wholeUnits*scale;
    }
    return ret;
};


// Duck is our core testing class :)
var Duck = Model.extend('Duck',{
    defaults: {
        age: 0,
        height: {type:MetricLengthField,value:'3cm'},
        mood: 'neutral'
    },
    // Simply a regular convenience method
    canDrink: function () {
        return this.age >= 18; // Russia
    },
    validate: function (spec,val) {
        return ''; // :|
        //return spec.op()!=='set' || !('height' in val);
        //throw new Error("can't set height, may only grow");
    },
    $$grow: function (spec,by,src) {
        this.height = this.height.add(by);
    }
});

var Nest = SyncSet.extend('Nest',{
    entryType: Duck
});

var storage2 = new Storage(false);
var host2 = env.localhost= new Host('gritzko',0,storage2);
host2.availableUplinks = function () {return [storage2]; };

asyncTest('2.a basic listener func', function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost= host2;
    expect(5);
    // construct an object with an id provided; it will try to fetch
    // previously saved state for the id (which is none)
    var huey = host2.get('/Duck#hueyA');
    //ok(!huey._version); //storage is a?sync
    // listen to a field
    huey.on('age',function lsfn2a (spec,val){  // FIXME: filtered .set listener!!!
        equal(val.age,1);
        equal(spec.op(),'set');
        equal(spec.toString(),'/Duck#hueyA!'+spec.version()+'.set');
        var version = spec.token('!');
        equal(version.ext,'gritzko');
        huey.off('age',lsfn2a);
        equal(huey._lstn.length,2); // only the uplink remains (and the comma)
        start();
    });
    huey.on('.init', function init2a () {
        huey.set({age:1});
    });
});

test('2.b create-by-id', function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost= host2;
    // there is 1:1 spec-to-object correspondence;
    // an attempt of creating a second copy of a model object
    // will throw an exception
    var dewey1 = new Duck('dewey');
    // that's we resort to descendant() doing find-or-create
    var dewey2 = host2.get('/Duck#dewey');
    // must be the same object
    strictEqual(dewey1,dewey2);
    equal(dewey1.spec().type(),'Duck');
});


test('2.c version ids', function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost= host2;
    var louie = new Duck('louie');
    var ts1 = host2.time();
    louie.set({age:3});
    var ts2 = host2.time();
    ok(ts2>ts1);
    var vid = louie._version.substr(1);
    ok(ts1<vid);
    ok(ts2>vid);
    console.log(ts1,vid,ts2);
});

test('2.d pojos',function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost= host2;
    var dewey = new Duck({age:0});
    var json = dewey.pojo();
    var duckJSON = {
        mood: "neutral",
        age: 0,
        height: '3cm'
    };
    deepEqual(json,duckJSON);
});

asyncTest('2.e reactions',function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost= host2;
    var huey = host2.get('/Duck#huey');
    expect(2);
    var handle = Duck.addReaction('age', function reactionFn(spec,val) {
        console.log('yupee im growing');
        equal(val.age,1);
        start();
    });
    //var version = host2.time(), sp = '!'+version+'.set';
    huey.deliver(huey.newEventSpec('set'), {age:1});
    Duck.removeReaction(handle);
    equal(Duck.prototype._reactions['set'].length,0); // no house cleaning :)
});

// TODO $$event listener/reaction (Model.on: 'key' > .set && key check)

test('2.f once',function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost= host2;
    var huey = host2.get('/Duck#huey');
    expect(1);
    huey.once('age',function onceAgeCb(spec,value){
        equal(value.age,4);
    });
    huey.set({age:4});
    huey.set({age:5});
});

test('2.g custom field type',function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost= host2;
    var huey = host2.get('/Duck#huey');
    huey.set({height:'32cm'});
    ok(Math.abs(huey.height.meters-0.32)<0.0001);
    var vid = host2.time();
    host2.deliver(new Spec('/Duck#huey!'+vid+'.set'),{height:'35cm'});
    ok(Math.abs(huey.height.meters-0.35)<0.0001);
});

test('2.h state init',function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost= host2;
    var factoryBorn = new Duck({age:0,height:'4cm'});
    ok(Math.abs(factoryBorn.height.meters-0.04)<0.0001);
    equal(factoryBorn.age,0);
});

test('2.i batched set',function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost= host2;
    var nameless = new Duck();
    nameless.set({
        age:1,
        height: '60cm'
    });
    ok(Math.abs(nameless.height.meters-0.6)<0.0001);
    equal(nameless.age,1);
    ok(!nameless.canDrink());

});

// FIXME:  spec - to - (order)
test('2.j basic Set functions (string index)',function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost= host2;
    var hueyClone = new Duck({age:2});
    var deweyClone = new Duck({age:1});
    var louieClone = new Duck({age:3});
    var clones = new Nest();
    clones.addObject(louieClone);
    clones.addObject(hueyClone);
    clones.addObject(deweyClone);
    var sibs = clones.list(function(a,b){return a.age - b.age;});
    strictEqual(sibs[0],deweyClone);
    strictEqual(sibs[1],hueyClone);
    strictEqual(sibs[2],louieClone);
    var change = {};
    change[hueyClone.spec()] = 0;
    clones.change(change);
    var sibs2 = clones.list(function(a,b){return a.age - b.age;});
    equal(sibs2.length,2);
    strictEqual(sibs2[0],deweyClone);
    strictEqual(sibs2[1],louieClone);
});

test('2.k distilled log', function (test) {
    function logSize(obj) {
        var log = obj._oplog, cnt=0;
        for(var key in log) { // jshint ignore:line
            cnt++;
        }
        return cnt;
    }
    console.warn(QUnit.config.current.testName);
    env.localhost= host2;
    var duckling1 = host2.get(Duck);
    duckling1.set({age:1});
    duckling1.set({age:2});
    duckling1.distillLog();
    equal(logSize(duckling1),1);
    duckling1.set({height:'30cm',age:3});
    duckling1.set({height:'40cm',age:4});
    duckling1.distillLog();
    equal(logSize(duckling1),1);
    duckling1.set({age:5});
    duckling1.distillLog();
    equal(logSize(duckling1),2);
});

test('2.l partial order', function (test) {
    env.localhost= host2;
    var duckling = new Duck();
    duckling.deliver(new Spec(duckling.spec()+'!time+user2.set'),{height:'2cm'});
    duckling.deliver(new Spec(duckling.spec()+'!time+user1.set'),{height:'1cm'});
    equal(duckling.height.toString(), '2cm');
});

asyncTest('2.m init push', function (test) {
    env.localhost= host2;
    var scrooge = new Duck({age:105});
    scrooge.on('.init', function check() {
        var tail = storage2.tails[scrooge.spec()];
        // FIXME equal(scrooge._version.substr(1), scrooge._id);
        var op = tail && tail[scrooge._version+'.set'];
        ok(tail) && ok(op) && equal(op.age,105);
        start();
    });
});

test('2.n local listeners for on/off', function () {
    console.warn(QUnit.config.current.testName);
    expect(5);
    env.localhost= host2;
    var duck = new Duck();
    duck.on('.on', function (spec, val) {
        console.log('triggered by itself, on(init) and host2.on below');
        equal(spec.op(), 'on');
    });
    duck.on('.init',function gotit(){
        console.log('inevitable');
        ok(duck._version);
    });
    duck.on('.reon', function (spec, val) {
        console.log("must NOT get triggered if the storage is sync");
        equal(spec.op(), 'reon');
    });
    host2.on('/Host#gritzko.on', function (spec, val) {
        console.log('this listener is triggered by itself');
        equal(spec.op(), 'on');
    });
});

/*  TODO
 * test('2.m on/off sub', function (test) {
    env.localhost= host2
    var duckling = new Duck();

    expect(2);
    duckling.on('on',function(spec){
        ok(spec.op(),'on');
    });
    duckling.on('set',function(spec){
        equal(spec.op(),'set');
    });
    duckling.set({age:1});

});*/

},{"../lib/Host":3,"../lib/Model":7,"../lib/Set":11,"../lib/Spec":12,"../lib/Storage":13,"../lib/env":17}],20:[function(require,module,exports){
"use strict";

var env = require('../lib/env');
var Host = require('../lib/Host');
var Model = require('../lib/Model');
var Storage = require('../lib/Storage');
require('../lib/AsyncLoopbackConnection');

env.multihost = true;
env.debug = console.log;

var Thermometer = Model.extend('Thermometer',{
    defaults: {
        t: -20 // Russia :)
    }
});


asyncTest('3.a serialized on, reon', function (){
    console.warn(QUnit.config.current.testName);
    var storage = new Storage(true);
    var uplink = new Host('swarm~3a',0,storage);
    var downlink = new Host('client~3a',5);
    // that's the default uplink.getSources = function () {return [storage]};

    uplink.accept('loopback:3a');
    downlink.connect('loopback:a3'); // TODO possible mismatch

    //downlink.getSources = function () {return [lowerPipe]};

    downlink.on('/Thermometer#room.init',function i(spec,val,obj){
        obj.set({t:22});
    });

    setTimeout(function x(){
        var o = uplink.objects['/Thermometer#room'];
        ok(o);
        o && equal(o.t,22);
        //downlink.disconnect(lowerPipe);
        start();
        downlink.disconnect();
    }, 250);

});


asyncTest('3.b pipe reconnect, backoff', function (){
    console.warn(QUnit.config.current.testName);
    var storage = new Storage(false);
    var uplink = new Host('swarm~3b', 0, storage);
    var downlink = new Host('client~3b');

    uplink.accept('loopback:3b');
    downlink.connect('loopback:b3'); // TODO possible mismatch

    var thermometer = uplink.get(Thermometer), i=0;

    // OK. The idea is to connect/disconnect it 100 times then
    // check that the state is OK, there are no zombie listeners
    // no objects/hosts, log is 1 record long (distilled) etc

    var ih = setInterval(function(){
        thermometer.set({t:i});
        if (i++==30) {
            ok(thermometer._lstn.length<=3); // storage and maybe the client
            clearInterval(ih);
            start();
            uplink.disconnect();
        }
    },100);

    // FIXME sets are NOT aggregated; make a test for that

    downlink.on(thermometer.spec().toString() + '.set', function i(spec,val,obj){
        if (spec.op()==='set') {
            var loopbackPipes = env.streams.loopback.pipes;
            var stream = loopbackPipes['b3'];
            stream && stream.close();
        }
    });

});



asyncTest('3.c Disconnection events', function () {
    console.warn(QUnit.config.current.testName);

    var storage = new Storage(true);
    var uplink = new Host('uplink~C',0,storage);
    var downlink1 = new Host('downlink~C1');
    //var downlink2 = new Host('downlink~C2');
    uplink.getSources = function () {
        return [storage];
    };
    downlink1.getSources = function () {
        return [uplink];
    };
    //downlink2.getSources = function () {return [uplink]};

    uplink.accept('loopback:3c');
    downlink1.connect('loopback:c3');

    env.localhost = downlink1;

    /*var miceA = downlink1.get('/Mice#mice');
    var miceB = downlink2.get('/Mice#mice');
    var mickey1 = downlink1.get('/Mouse');*/

    expect(3);

    downlink1.on('.reoff', function (spec,val,src) {
        equal(src, downlink1);
        ok(!src.isUplinked());
        start();
    });

    downlink1.on('.reon', function (spec,val,src) {
        equal(spec.id(), 'downlink~C1');
        setTimeout(function(){ //:)
            downlink1.disconnect('uplink~C');
        }, 100);
    });
});

},{"../lib/AsyncLoopbackConnection":1,"../lib/Host":3,"../lib/Model":7,"../lib/Storage":13,"../lib/env":17}],21:[function(require,module,exports){
"use strict";

var env = require('../lib/env');
var Spec = require('../lib/Spec');
var Host = require('../lib/Host');
var Text = require('../lib/Text');
var Storage = require('../lib/Storage');


test('4._ diff', function (test){
    var eq = Text.diff('same','same');
    deepEqual(eq,[['=','same']]);
    var ch = Text.diff('was','now');
    deepEqual(ch,[['+','now'],['-','was']]);
    var mid = Text.diff('muddle','middle');
    deepEqual(mid,[['=','m'],['+','i'],['-','u'],['=','ddle']]);
});

var storage = new Storage(false);
var host04 = new Host('gritzko',0,storage);
host04.availableUplinks = function () {return [storage];};

test('4.a init', function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost = host04;

    var text = new Text();
    text.set('test');
    equal(text.text,'test');
});

test('4.b in rm', function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost = host04;

    var text = new Text();

    text.set("tesst");
    text.set("tet");
    text.set("text");

    equal(text.text,'text');
    equal(text.weave,'\ntexs\u0008s\u0008t');

    text.set('terminator model t');
    equal(text.text,'terminator model t');
});

test('4.c concurrent insert', function (test) {
    console.warn(QUnit.config.current.testName);
    env.localhost = host04;

    var text = new Text('ALE');
    text.deliver ( new Spec("/Text#ALE!00001+gritzko.insert"), { '00000+swarm': 'a' });
    text.deliver ( new Spec("/Text#ALE!00003+gritzko~1.insert"), { '00001+gritzko' : 'l' });
    text.deliver ( new Spec("/Text#ALE!00002+gritzko~2.insert"), { '00001+gritzko' : 'e' });
    equal(text.text,'ale');


});

},{"../lib/Host":3,"../lib/Spec":12,"../lib/Storage":13,"../lib/Text":15,"../lib/env":17}],22:[function(require,module,exports){
"use strict";

var env = require('../lib/env');
var Spec = require('../lib/Spec');
var LongSpec = require('../lib/LongSpec');
var Host = require('../lib/Host');
//var MinutePreciseClock = require('../lib/MinutePreciseClock');

test('5.a unicode (base 2^15) numbers', function (test) {
    // encode
    var numf = Spec.base2int('ABCDE'); // 6*6 = 36 bit, 3 uni
    var num = (0xE) + (0xD<<6) + (0xC<<12) + (0xB<<18) + (0xA<<24);
    var suffix = 0xf;
    equal(numf,num);
    var uni = String.fromCharCode(0x30+(numf>>15), 0x30+(numf&0x7fff));
    var unif = LongSpec.int2uni(numf);
    equal(unif,uni);
    // decode
    var de = LongSpec.uni2int(unif);
    equal(de,numf);

    for(var i=0; i<=0x7fff; i++) {
        var u = LongSpec.int2uni(i);
        var i2 = LongSpec.uni2int(u);
        if (i!==i2) { equal(i,i2); }
    }
});

test('5.b constructor', function (test){
    var ls = new LongSpec('/ab#cd!ef+gh');
    ls.add('.ij');
    var str = ls.toString();
    equal(str, '/ab#cd!ef+gh.ij');
});

test('5.c1 encode - sequences', function (test){

    var ls = new LongSpec('#unencodeable#unencodeable#unencodeable');
    equal(ls.chunks[0],'#unencodeable##')

    var book = {en:{'.on':'.0','+ext':'+e'}};
    var repeats = new LongSpec('.on.on.on.on.on.on.on.on.on.on', book);
    equal(repeats.chunks[0], '.0.........');

    var numbers = new LongSpec('!shrt1!shrt2!shrt3');
    var uni = LongSpec.int2uni(Spec.base2int('shrt1'));
    equal(numbers.chunks[0], '!'+uni+'!!');

    var exts = new LongSpec('!shrt1+ext!shrt2+ext!shrt3+ext', book);
    equal(exts.chunks[0], '!'+uni+'+e!!');

    var longs = new LongSpec('#longnum001#longnum002');
    equal(longs.chunks[0].length, 6);

});

test('5.c3 general encode/decode', function (test){
    var codeBook = {
        en : {
            '/Mouse': '/M',
            '.on':    '.o',
            '.off':   '.f',
            '#Mickey':'#i'
        }
    };
    var spec1 = '/Mouse#Mickey!abcde.on';
    var ls1 = new LongSpec(spec1,codeBook);
    equal(ls1.chunks[0],'/M#i!\u4b64\u7a59.o');
    equal(spec1,ls1.toString());

    var spec2 = '/Mouse#Mickey!bcdef.off';
    var ls2 = new LongSpec(spec2,codeBook);
    equal(ls2.chunks[0],'/M#i!\u4d6d\u0a9a.f');
    equal(spec2,ls2.toString());

    var spec3 = '/Mouse#abcde.off';
    var ls3 = new LongSpec(spec3,codeBook);
    equal(ls3.chunks[0],'/M#\u4b64\u7a59.f');
    equal(spec3,ls3.toString());

    var zeros5 = new LongSpec('.00000');
    equal(zeros5.toString(),'.00000');

    var zeros7 = new LongSpec('.0000001');
    equal(zeros7.toString(),'.0000001');

});

test('5.d find & partials', function (test){
    var ambils = new LongSpec('!one!two.three/TimesTwo.three.the#end+it~is~not#end');
    var the = ambils.find('.the');
    equal(the.index,5);
    var three = ambils.find('.three');
    ok(three.index,2);
    var three2 = ambils.find('.three',three.index+1);
    equal(three2.index, 4);
    var none = ambils.find('.Ti');
    ok(none.end()); // FIXME make foolproof
    var last = ambils.find('#end');
    equal(last.index,7);
});

test('5.e edits and O(n)', function (test){
    var count = 4; // inf loop protection
    var longls = new LongSpec();
    for(var i=0; i<count; i++) {
        longls.append('.bc');
    }
    var at;
    while ( (at = longls.find('.bc',at?at.index+1:at)) && count--) {
        at.insert('.a');
    }
    var spec = longls.toString();
    equal(spec, '.a.bc.a.bc.a.bc.a.bc');
});


/*test('5.g mass encode/decode', function (test) {
    var epoch = 1262275200000; // TODO move to Spec
    var time = ((new Date().getTime()-epoch)/1000)|0;
    for(var i=0; i<100; i++) {
        var t = time + i;
        var ts = Spec.int2base(t);
        var spec = '/Test#05LongSpec!'+ts+'.on';
        var enc = new LongSpec(spec);
        var dec = enc.toString();
        equal(spec,dec);
    }
});

test('5.h Array-like API', function(test) {
    var ls = new LongSpec('!one#two#two+andahalf.three/four4');
    var three = ls.itemAt(3);
    equal(three,'.three');
    var i = ls.indexOf('/four4');
    equal(i,4);
    var j = ls.indexOf('#two+andaquarter');
    equal(j,-1);
    var at = ls._at(1);
    equal(ls.decode(at.en), '#two');
    ls.splice(1,3,'.23');
    equal(ls.toString(),'!one.23/four4');
});*/

test('5.i Iterator', function(test) {
    var ls = new LongSpec('!one#two.three/four4');
    var i = ls.iterator();
    equal(i.token(),'!one');
    i.next();
    equal(i.token(),'#two');

    var e = ls.iterator();
    e.skip(3);
    equal(e.token(),'/four4');
    e.next();
    ok(e.end());
    equal(e.token(),undefined);

    var e300 = ls.iterator();
    e300.skip(300);
    ok(e300.end());
    equal(e300.token(),undefined);

    var lx = new LongSpec('!one#two.three/four4');
    var x = lx.iterator();
    x.skip(1);
    x.erase(2);
    equal(lx.toString(),'!one/four4'); // 10

    var j = ls.iterator(); // still !one#two.three/four4
    j.skip(2);
    equal(j.token(),'.three');
    j.erase(2);
    equal(j.index,2); // eof
    j.insert('#two+andahalf');
    equal(j.index,3);
    equal(ls.length(),3);
    equal(ls.toString(),'!one#two#two+andahalf');

    var k = ls.iterator();
    k.skip(2);
    equal(k.token(),'#two+andahalf');

    var l = ls.iterator(); // !one#two#two+andahalf
    equal(l.index,0);
    l.insert('/zero');
    equal(ls.length(),4);
    equal(l.token(),'!one');
    equal(l.index,1);

    var empty = new LongSpec();
    var ei = empty.iterator();
    ok(ei.end());
    ei.insert('!something+new'); // FIXME throw on format violation
    equal(empty.toString(), '!something+new');
});

test('5.j Sequential compression', function(test) {
    var ls = new LongSpec('!00abc!00abd!00abe!00abf');
    var i = ls.end();
    i.insertBlock('!00abg!00abh!00abi');
    equal(ls.length(),7);
    ok(ls.charLength()<10);
    var f = ls.find('!00abe');
    equal(f.index,2);
    var v = ls.find('!00abh');
    equal(v.index,5);
    var j = ls.iterator(4);
    equal(j.token(),'!00abg');

    var lse = new LongSpec('!~~~a1+src!~~~a2+src!~~~a3+src');
    var ei = lse.iterator();
    ei.next();
    equal(ei.token(),'!~~~a2+src');
    equal(ei.match[0].length,1);
});

// test TODO   max-uni insert
// test TODO   multiple inserts, iterator wear

/*test('5.7 sequential coding', function (test) {
    var clockContext = {
        maxTimestampSeen: '',
        lastTimestampAssigned: '',
        lastTimeAssigned: '',
        lastSeqAssigned: 0
    };
    Swarm.env.clock = MinutePreciseClock;
    // install minute-precise timestamps
    var ls = new LongSpec();
    var seqs = [];
    for(var i=0; i<10; i++) {
        var time = seqs.push(Swarm.env.clock.timestamp(clockContext));
        var id = '!' + time + '+somehost';
        seqs[i] = id;
    }
    for(var i=0; i<10; i++) {
        ls.append(seqs[i]);
    }
    ok(ls.value.length<30);
    var iter = ls.iterator();
    for(var i=0; i<10; i++) {
        ok(iter);
        equal(ls._dicode(iter.en), seqs[i]);
        iter = iter.next();
    }
    ok(iter===undefined);
    var find = ls._find(seqs[3]);
    ok(find);
    var next = find.next();
    equal(ls.decode(next.en), seqs[4]);
    Swarm.env.clock = Swarm.SecondPreciseClock;
});*/

test('5.8 humane API: insert, insertAfter', function (test){
    var ls = new LongSpec('!one#two/four+4');
    ls.insert('#two+andahalf',2);
    ls.insert('.three',3);
    equal(ls.tokenAt(4),'/four+4');
    equal(ls.toString(), '!one#two#two+andahalf.three/four+4');
});

},{"../lib/Host":3,"../lib/LongSpec":5,"../lib/Spec":12,"../lib/env":17}],23:[function(require,module,exports){
"use strict";

// This test suite covers various handshake patterns.
// Making an object live demands a connection to an uplink.
// A connection starts with a handshake synchronizing versions on both ends.
// Depending on who has state and who does not, versions on both ends,
// also various concurrency/asynchrony issues, handshakes proceed in different
// ways.

var env = require('../lib/env');
var Spec = require('../lib/Spec');
var Host = require('../lib/Host');
var Model = require('../lib/Model');
var Storage = require('../lib/Storage');
require('./model/Mice');

env.multihost = true;

/** Must be constructed from String, serialized into a String.
    JSON string is OK :) */
function FullName (name) {
    var m = name.match(/(\S+)\s+(.*)/);
    this.first = m[1];
    this.last = m[2];
}
FullName.prototype.toString = function () {
    return this.first + ' ' + this.last;
};

var Mouse = Model.extend('Mouse', {
    defaults: {
        x: 0,
        y: 0
        //name: FullName
    },
    // adapted to handle the $$move op
    TODO_distillLog: function () {
        // explain
        var sets = [],
            cumul = {},
            heads = {},
            spec;
        for(spec in this._oplog) {
            if (Spec.get(spec, '.') === '.set') {
                sets.push(spec);
            }
        }
        sets.sort();
        for(var i=sets.length-1; i>=0; i--) {
            spec = sets[i];
            var val = this._oplog[spec], notempty=false;
            for(var key in val) {
                if (key in cumul) {
                    delete val[key];
                } else {
                    notempty = cumul[key] = true;
                }
            }
            var source = new Spec(key).source();
            notempty || (heads[source] && delete this._oplog[spec]);
            heads[source] = true;
        }
        return cumul;
    },
    ops: {
        move: function (spec,d) {
            // To implement your own ops you must understand implications
            // of partial order; in this case, if an op comes later than
            // an op that overwrites it then we skip it.
            var version = spec.version();
            if (version<this._version) {
                for(var opspec in this._oplog) {
                    if (opspec > '!' + version) {
                        var os = new Spec(opspec);
                        if (os.op() === 'set' && os.version() > version) {
                            return; // overwritten in the total order
                        }
                    }
                }
            }
            // Q if set is late => move is overwritten!
            this.x += d.x||0;
            this.y += d.y||0;
        }
    }
});

//    S O  I T  F I T S

asyncTest('6.a Handshake K pattern', function () {
    console.warn(QUnit.config.current.testName);

    var storage = new Storage(true);
    // FIXME pass storage to Host
    var uplink = new Host('uplink~K',0,storage);
    var downlink = new Host('downlink~K',100); 
    uplink.getSources = function () {return [storage];};
    downlink.getSources = function () {return [uplink];};
    uplink.on(downlink);

    env.localhost = uplink;
    var uprepl = new Mouse({x:3,y:3});
    downlink.on(uprepl.spec()+'.init',function(sp,val,obj){
        //  ? register ~ on ?
        //  host ~ event hub
        //    the missing signature: x.emit('event',value),
        //      x.on('event',fn)
        //    host.on(Mouse,fn)
        //    host.on(Mouse) -- is actually a value
        //  on() with a full filter:
        //    /Mouse#Mickey!now.on   !since.event   callback
        //  host's completely specd filter
        //    /Host#local!now.on   /Mouse#Mickey!since.event   callback
        equal(obj.x,3);
        equal(obj.y,3);
        equal(obj._version,uprepl._version);
        // TODO this happens later ok(storage..init[uprepl.spec()]);
        start();
    });
    //var dlrepl = downlink.objects[uprepl.spec()];

    // here we have sync retrieval, so check it now
    //equal(dlrepl.x,3);
    //equal(dlrepl.y,3);
    //equal(dlrepl._version,dlrepl._id);
    // NO WAY, storage is async
});


asyncTest('6.b Handshake D pattern', function () {
    console.warn(QUnit.config.current.testName);

    var storage = new Storage(true);
    var uplink = new Host('uplink~D',0,storage);
    var downlink = new Host('downlink~D',10000);
    uplink.getSources = function () {return [storage];};
    downlink.getSources = function () {return [uplink];};
    uplink.on(downlink);
    env.localhost = downlink;

    storage.states['/Mouse#Mickey'] = JSON.stringify({
        x:7,
        y:7,
        _version: '!0eonago',
        _oplog:{
            '!0eonago.set': {x:7,y:7}
        }
    });

    // TODO
    //  * _version: !v1!v2
    //    v * add Spec.Map.toString(trim) {rot:ts,top:count}
    //      * if new op !vid was trimmed => add manually
    //      * if new op vid < _version => check the log (.indexOf(src))
    //    v * sort'em
    //  * clean $$set
    //  * add testcase: Z-rotten
    //      * old replica with no changes (no rot)
    //      * old repl one-side changes
    //      * old repl two-side changes (dl is rotten)
    //  * document it
    //  * "can't remember whether this was applied" situation
    //      * high concurrency offline use
    //
    //  The underlying assumption: either less than 5 entities
    //  touch it or they don't do it at once (if your case is
    //  different consider RPC impl)
    //  Model.ROTSPAN
    //  Model.COAUTH

    downlink.on('/Mouse#Mickey.init',function(spec,val,obj){
        equal(obj._id,'Mickey');
        equal(obj.x,7);
        equal(obj.y,7);
        equal(obj._version,'!0eonago');
        start();
    });
    var dlrepl = downlink.objects['/Mouse#Mickey'];

    // storage is async, waits a tick
    ok(!dlrepl.x);
    ok(!dlrepl.y);

});

// both uplink and downlink have unsynchronized changes
asyncTest('6.c Handshake Z pattern', function () {
    console.warn(QUnit.config.current.testName);

    var storage = new Storage(false);
    var oldstorage = new Storage(false);
    var uplink = new Host('uplink~Z',0,storage);
    var downlink = new Host('downlink~Z');
    uplink.getSources = function () {return [storage];};
    downlink.getSources = function () {return [oldstorage];};

    var oldMickeyState = {
        x:7,
        y:7,
        _version: '!0eonago',
        _oplog:{
            '!0eon+ago.set' : {y:7},
            '!000ld+old.set': {x:7}
        }
    };
    storage.states['/Mouse#Mickey'] = JSON.stringify(oldMickeyState);
    oldstorage.states['/Mouse#Mickey'] = JSON.stringify(oldMickeyState);

    // new ops at the uplink' storage
    storage.tails['/Mouse#Mickey'] =
        JSON.stringify({
            '!1ail+old.set': {y:10}
        });

    env.localhost = downlink;

    var dlrepl = new Mouse('Mickey',oldMickeyState);
    uplink.on('/Mouse#Mickey');
    var uprepl = uplink.objects[dlrepl.spec()];

    // offline changes at the downlink
    dlrepl.set({x:12});

    // ...we see the tail applied, downlink changes not here yet
    equal(uprepl.x,7);
    equal(uprepl.y,10);

    // Two uplinks! The "server" and the "cache".
    downlink.getSources = function () { return [oldstorage,uplink]; };
    console.warn('connect');
    uplink.on(downlink);

    // their respective changes must merge
    equal(dlrepl.x,12);
    equal(dlrepl.y,10);
    equal(uprepl.x,12);
    equal(uprepl.y,10);

    start();

});


asyncTest('6.d Handshake R pattern', function () {
    console.warn(QUnit.config.current.testName);

    var storage = new Storage(false);
    var uplink = new Host('uplink~R');
    var downlink = new Host('downlink~R');
    uplink.getSources = function () {return [storage];};
    downlink.getSources = function () {return [uplink];};
    uplink.on(downlink);
    env.localhost = downlink;

    downlink.on('/Mouse#Mickey.init',function(spec,val,dlrepl){
        // there is no state in the uplink, dl provided none as well
        ok(!dlrepl.x);
        ok(!dlrepl.y);
        equal(dlrepl._version,'!0'); // auth storage has no state

        dlrepl.set({x:18,y:18}); // FIXME this is not R
        var uprepl = uplink.objects['/Mouse#Mickey'];
        equal(uprepl.x,18);

        start();
    });

});


asyncTest('6.e Handshake A pattern', function () {
    console.warn(QUnit.config.current.testName);

    var storage = new Storage(false);
    var uplink = new Host('uplink~A');
    var downlink = new Host('downlink~A');
    uplink.getSources = function () {return [storage];};
    downlink.getSources = function () {return [uplink];};
    uplink.on(downlink);
    env.localhost = downlink;

    var mickey = new Mouse({x:20,y:20});
    equal(mickey._id, mickey._version.substr(1));

    // FIXME no value push; this is R actually
    setTimeout(function check(){
        var uprepl = uplink.objects[mickey.spec()];
        var dlrepl = downlink.objects[mickey.spec()];
        equal(uprepl.x,20);
        equal(uprepl.y,20);
        equal(dlrepl.x,20);
        equal(dlrepl.y,20);
        start();
    }, 100);

});


test('6.f Handshake and sync', function () {
    console.warn(QUnit.config.current.testName);

    var storage = new Storage(false);
    var uplink = new Host('uplink~F',0,storage);
    var downlink1 = new Host('downlink~F1');
    var downlink2 = new Host('downlink~F2');
    uplink.getSources = function () {return [storage];};
    downlink1.getSources = function () {return [uplink];};
    downlink2.getSources = function () {return [uplink];};

    uplink.on(downlink1);

    env.localhost = downlink1;

    var miceA = downlink1.get('/Mice#mice');
    var miceB = downlink2.get('/Mice#mice');

    var mickey1 = downlink1.get('/Mouse');
    var mickey2 = downlink2.get('/Mouse');
    miceA.addObject(mickey1);

    uplink.on(downlink2);

    var mickey1at2 = miceB._objects[mickey1.spec()];
    ok(miceA._objects[mickey1.spec()]);
    ok(mickey1at2);
    miceB.addObject(mickey2);

    var mickey2at1 = miceA._objects[mickey2.spec()];
    ok(miceB._objects[mickey2.spec()]);
    ok(mickey2at1);

    mickey1.set({x:0xA});
    mickey2.set({x:0xB});
    equal(mickey1at2.x,0xA);
    equal(mickey2at1.x,0xB);

    mickey1at2.set({y:0xA});
    mickey2at1.set({y:0xB});
    equal(mickey1.y,0xA);
    equal(mickey2.y,0xB);
});



asyncTest('6.g Cache vs storage',function () {
    var storage = new Storage(true);
    var cache = new Storage(false);
    cache.isRoot = false;
    var uplink = new Host('uplink~G',0,storage);
    var downlink = new Host('downlink~G',0,cache);
    downlink.getSources = function () {return [uplink];};

    env.localhost = uplink;
    var mickey = new Mouse({x:1,y:2});

    //env.localhost = downlink;
    var copy = downlink.get(mickey.spec());
    copy.on('.init', function (){
        equal(copy.x,1);
        equal(copy.y,2);
        start();
    });

});

},{"../lib/Host":3,"../lib/Model":7,"../lib/Spec":12,"../lib/Storage":13,"../lib/env":17,"./model/Mice":26}],24:[function(require,module,exports){
"use strict";

var env = require('../lib/env');
var Spec = require('../lib/Spec');
var Model = require('../lib/Model');
var Vector = require('../lib/Vector');
var Host = require('../lib/Host');
var Storage = require('../lib/Storage');

var Agent = Model.extend('Agent', {
    defaults: {
        name: 'Anonymous',
        num: -1,
        gun: "IMI Desert Eagle",
        dressCode: "Business"
    }
});

var vhost = new Host('matrix',0);

env.localhost = vhost;

var smith = new Agent({name:'Smith', num:1});
var jones = new Agent({name:'Jones', num:2});
var brown = new Agent({name:'Brown', num:3});

var AgentVector = Vector.extend('AgentVector',{
    objectType: Agent
});

function checkOrder(vec) {
    var names = [];
    vec._objects.forEach(function(o){ names.push(o.name); });
    equal(names.join(), 'Smith,Jones,Brown');
}

test('7.a init vector', function (test) {
    env.localhost = vhost;
    var vec = new Vector();
    vec.insert(smith);
    vec.insert(brown,0);
    vec.insert(jones,smith);
    checkOrder(vec);
});

/*test('7.b ordered insert', function (test) {
    env.localhost = vhost;
    var vector = new Vector();
    function order(a,b) {
        return a.num - b.num;
    }
    vector.setOrder(order);
    vector.insert(jones);
    vector.insert(smith);
    vector.insert(brown);
    checkOrder(vec);
});*/

test('7.c insert/remove', function (test) {
    env.localhost = vhost;
    var vec = new AgentVector();
    // default object type
    vec.insert(smith);
    vec.insertAfter(brown._id,smith);
    vec.remove(smith);
    vec.insert(jones.spec());
    vec.insertBefore(smith,jones.spec());
    checkOrder(vec);
});

test('7.d concurrent insert', function (test) {
    env.localhost = vhost;
    function cb () {
        throw new Error('what?');
    }

    var vec = new Vector('vecid');
    var smithOp = Spec.as(vec.insert(smith)).tok('!');
    var t1 = vhost.time().replace('+'+vhost._id, '+src2');
    var t2 = vhost.time().replace('+'+vhost._id, '+src1');

    vec.deliver ('/Vector#vecid!'+t2+'.in', jones.spec()+smithOp, cb);
    vec.deliver ('/Vector#vecid!'+t1+'.in', brown.spec()+smithOp, cb);
    checkOrder(vec);
    equal(vec._order.toString(), smithOp+'!'+t2+'!'+t1);

    var vec2 = new AgentVector('vecid2');
    var smithOp2 = Spec.as(vec2.insert(smith)).tok('!');
    t1 = vhost.time().replace('+'+vhost._id, '+src2');
    t2 = vhost.time().replace('+'+vhost._id, '+src1');

    vec2.deliver ('/Vector#vecid2!'+t1+'.in', brown.spec()+smithOp2, cb);
    vec2.deliver ('/Vector#vecid2!'+t2+'.in', jones.spec()+smithOp2, cb);
    checkOrder(vec2);
    equal(vec2._order.toString(), smithOp2+'!'+t2+'!'+t1);
});

test('7.e dead point', function (test) {
    env.localhost = vhost;
    var vec = new Vector();
    // keeps
    vec.insert(smith);
    var pos = vec._order.tokenAt(0); // !time
    vec.remove(smith);
    var t1 = vhost.time().replace('+'+vhost._id, '+src2');
    var t2 = vhost.time().replace('+'+vhost._id, '+src1');
    function cb () {
        // nothing
    }
    vec.deliver(vec.spec()+'!'+t2+'.in', jones.spec()+pos, cb);
    vec.deliver(vec.spec()+'!'+t1+'.in', brown.spec()+pos, cb);
    vec.insertBefore(smith,jones);
    checkOrder(vec);
});

/*test('7.f splits: O(N^2) prevention', function (test) {
    // ONE! MILLION! ENTRIES!
    env.localhost = vhost;
    var vec = new Vector();
    // insert 1mln entries at pos i%length
    // TODO O(N^0.5) offset anchors
});*/

/*test('7.g log compaction', function (test) {   TODO HORIZON
    // values essentially reside in the _oplog
    // compaction only brings benefit on numerous repeated rewrites
    // over long periods of time (>EPOCH)
    env.localhost = vhost;
    var vec = new Vector();
    // /Type#elem ( !pos (offset)? )?
}); */

test('7.h duplicates', function (test) {
    env.localhost = vhost;
    var vec = new AgentVector();
    vec.insert(smith);
    vec.insertAfter(smith._id);
    vec.insertAfter(smith.spec()); // take that :)
    equal(vec._objects[0],smith);
    equal(vec._objects[1],smith);
    equal(vec._objects[2],smith);
});

test('7.l event relay', function (test) {
    var ids = [];
    var vec = new AgentVector();
    vec.insert(smith);
    vec.insert(smith);
    vec.insert(smith);
    vec.onObjectEvent(function(spec,val,src){
        ids.push(src.name);
    });
    smith.set({weapon:'bug'});
    equal(ids.join(),'Smith,Smith,Smith');
    vec.remove(1);
    //vec.move(1,0);
    ids = [];
    smith.set({weapon:'mighty fist'});
    equal(ids.join(),'Smith,Smith');
});


test('7.i Array-like API', function (test) {
    env.localhost = vhost;
    var vec = new AgentVector();
    vec.append(smith);
    vec.append(smith);
    vec.append(smith);
    vec.append(brown);
    equal(vec.indexOf(brown._id),3);
    equal(vec.indexOf(brown.spec()),3);
    equal(vec.indexOf(brown),3);
    equal(vec.indexOf(smith._id),0);
    equal(vec.indexOf(smith._id,1),1);
    //vec.splice(1,2,jones);
    //checkOrder(vec);
});

/*test('7.j sugary API', function (test) {
    var vec = new Vector();
    vec.insert(jones);
    vec.insertAfter(smith,jones);
    vec.insertBefore(brown,smith);
    vec.move('smith',0);
    checkOrder(vec);
    var i = vec.iterator();
    equal(i.object.name,'Smith');
    i.next();
    equal(i.object.name,'Jones');
    i.next();
    equal(i.object.name,'Brown');
});*/

/*test('7.k long Vector O(N^2)', function (test){
    var vec = new Vector();
    var num = 500, bignum = num << 1; // TODO 1mln entries (need chunks?)
    for(var i=0; i<bignum; i++) { // mooore meee!!!
        vec.append(smith);
    }
    for(var i=bignum-1; i>0; i-=2) {
        vec.remove(i);
    }
    equal(vec.length(), bignum>>1);
    equal(vec._objects[0].name,'Smith');
    equal(vec._objects[num-1].name,'Smith');
});*/

test('7.l onObjectEvent / offObjectEvent', function () {
    env.localhost = vhost;
    var vec = new AgentVector();
    vec.insert(smith);
    expect(2);

    function onAgentChanged() {
        ok(true);
    }

    vec.onObjectEvent(onAgentChanged);
    smith.set({dressCode: 'Casual'});
    smith.set({gun: 'nope'});

    vec.offObjectEvent(onAgentChanged);
    smith.set({gun: 'IMI Desert Eagle'});
});

asyncTest('7.m onObjectStateReady', function () {
    var asyncStorage = new Storage(true);
    env.localhost = null;
    var host = new Host('async_matrix', 0, asyncStorage);
    env.localhost = host;

    var vec = host.get('/AgentVector#test7l');

    var agents = [];
    for (var i = 0; i < 10; i++) {
        var agent = host.get('/Agent#smith_' + i);
        agents.push(agent);
        vec.insert(agent);
    }

    expect(21);

    // not inited at the beginning (+10 assertions)
    agents.forEach(function (agent) {
        ok(!agent._version);
    });


    vec.onObjectStateReady(function () {
        // check vector and all its entries inited (+1 assertion)
        ok(!!vec._version);
        // (+10) assertions
        agents.forEach(function (agent) {
            ok(!!agent._version);
        });

        start();
    });
});

},{"../lib/Host":3,"../lib/Model":7,"../lib/Spec":12,"../lib/Storage":13,"../lib/Vector":16,"../lib/env":17}],25:[function(require,module,exports){
require('./01_Spec');
require('./02_EventRelay');
require('./03_OnOff');
require('./04_Text');
require('./05_LongSpec');
require('./06_Handshakes');
require('./07_Vector');

},{"./01_Spec":18,"./02_EventRelay":19,"./03_OnOff":20,"./04_Text":21,"./05_LongSpec":22,"./06_Handshakes":23,"./07_Vector":24}],26:[function(require,module,exports){
"use strict";

var SyncSet = require('../../lib/Set');

// this collection class has no functionality except for being a list
// of all mice currently alive; we'll only use one singleton object
// set mixin
module.exports = SyncSet.extend('Mice', {

});

},{"../../lib/Set":11}]},{},[25]);
