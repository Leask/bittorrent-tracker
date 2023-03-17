import { utilitas } from 'utilitas'
import arrayRemove from 'unordered-array-remove'
import Debug from 'debug'
import LRU from 'lru'
import randomIterate from 'random-iterate'

const debug = Debug('bittorrent-tracker:swarm')

const getLru = (options) => {
  if (!globalThis?.swarmHijack) { return new LRU(options); }
  const [EXCLUDED, SKIP_ECHO] = [['httpReq', 'httpRes'], { skipEcho: true }];
  return new class DbLru {
    constructor(options) {
      this.max = options?.max;
      this.maxAge = options?.maxAge;
      this.infoHash = options?.infoHash;
      this.dbio = globalThis.swarmHijack.dbio;
      this.table = globalThis.swarmHijack.table;
      this.lastCheck = 0;
      this.events = [];
    };

    on (event, callback) {
      return this.events.push({ event, callback });
    };

    async set (_id, peer, options) {
      return await this.dbio.upsert(this.table, {
        ...peer, _id, deleted: false, infoHash: this.infoHash,
        peer: utilitas.exclude(options._.peer, EXCLUDED),
        socket: peer.socket || null, updatedAt: new Date(),
      }, SKIP_ECHO);
    };

    async touch (_id) {
      return await this.dbio.execute(
        'UPDATE `' + this.table + '` SET `deleted` = ?, `updatedAt` = ? WHERE '
        + '`infoHash` = ? AND `_id` = ?', [false, new Date(), this.infoHash, _id]
      );
    };

    async peek (_id) {
      return await this.dbio.queryOne(
        'SELECT * FROM `' + this.table + '` WHERE `infoHash` = ? '
        + 'AND `_id` = ? AND `deleted` = ?', [this.infoHash, _id, false]
      );
    };

    async get (_id) {
      await this.touch(_id);
      return await this.peek(_id);
    };

    async trigger (event, value) {
      for (let item of this.events.filter(x => x.event === event)) {
        await item.callback({ key: value._id, value });
      }
    };

    async keys () {
      const [now, old, res] = [new Date(), [], []];
      const [lastValid, checkEvict] = [
        now - this.maxAge,
        now - this.lastCheck > 1000 * 60 && (this.lastCheck = now)
      ]; // check evict peers every 1 minute
      const peers = (await this.dbio.query(
        'SELECT `id`, `_id`, `infoHash`, `peerId`, `type`, `ip`, `socket`, '
        + '`port`, `complete`, `updatedAt` FROM `' + this.table + '` WHERE '
        + '`infoHash` = ? AND `deleted` = ?', [this.infoHash, false]
      )).sort((x, y) => y.updatedAt - x.updatedAt);
      for (let peer of peers) {
        if (peer.updatedAt > lastValid && res.length < this.max) {
          res.push(peer._id);
        } else if (checkEvict) {
          old.push(peer.id);
          await this.trigger('evict', peer);
        }
      }
      old.length && await this.dbio.updateByKeyValue(
        this.table, 'id', old, { deleted: true, updatedAt: now }, SKIP_ECHO
      );
      return res;
    };

    async remove (_id) {
      return this.dbio.execute(
        'UPDATED `' + this.table + '` SET `deleted` = ?, `updatedAt` = ? WHERE '
        + '`infoHash` = ? AND `_id` = ?', [true, new Date(), this.infoHash, _id]
      );
    }
  }(options);
};

// Regard this as the default implementation of an interface that you
// need to support when overriding Server.createSwarm() and Server.getSwarm()
class Swarm {
  constructor(infoHash, server) {
    const self = this
    self.infoHash = infoHash
    self.complete = 0
    self.incomplete = 0

    self.peers = getLru({
      max: server.peersCacheLength || 1000,
      maxAge: server.peersCacheTtl || 20 * 60 * 1000, // 20 minutes
      infoHash,
    })

    // When a peer is evicted from the LRU store, send a synthetic 'stopped' event
    // so the stats get updated correctly.
    self.peers.on('evict', async data => {
      const peer = data.value
      const params = {
        type: peer.type,
        event: 'stopped',
        numwant: 0,
        peer_id: peer.peerId
      }
      await self._onAnnounceStopped(params, peer, peer.peerId)
      peer.socket = null
    })
  }

  async announce (params, cb) {
    const self = this
    const id = params.type === 'ws' ? params.peer_id : params.addr
    // Mark the source peer as recently used in cache
    const peer = await self.peers.get(id)

    if (params.event === 'started') {
      await self._onAnnounceStarted(params, peer, id)
    } else if (params.event === 'stopped') {
      await self._onAnnounceStopped(params, peer, id)
      if (!cb) return // when websocket is closed
    } else if (params.event === 'completed') {
      await self._onAnnounceCompleted(params, peer, id)
    } else if (params.event === 'update') {
      await self._onAnnounceUpdate(params, peer, id)
    } else if (params.event === 'paused') {
      await self._onAnnouncePaused(params, peer, id)
    } else {
      cb(new Error('invalid event'))
      return
    }
    cb(null, {
      complete: self.complete,
      incomplete: self.incomplete,
      peers: await self._getPeers(params.numwant, params.peer_id, !!params.socket)
    })
  }

  scrape (params, cb) {
    cb(null, {
      complete: this.complete,
      incomplete: this.incomplete
    })
  }

  async _onAnnounceStarted (params, peer, id) {
    if (peer) {
      debug('unexpected `started` event from peer that is already in swarm')
      return await this._onAnnounceUpdate(params, peer, id) // treat as an update
    }

    if (params.left === 0) this.complete += 1
    else this.incomplete += 1
    await this.peers.set(id, {
      type: params.type,
      complete: params.left === 0,
      peerId: params.peer_id, // as hex
      ip: params.ip,
      port: params.port,
      socket: params.socket // only websocket
    }, { _: { peer: params } })
  }

  async _onAnnounceStopped (params, peer, id) {
    if (!peer) {
      debug('unexpected `stopped` event from peer that is not in swarm')
      return // do nothing
    }

    if (peer.complete) this.complete -= 1
    else this.incomplete -= 1

    // If it's a websocket, remove this swarm's infohash from the list of active
    // swarms that this peer is participating in.
    if (peer.socket && !peer.socket.destroyed) {
      const index = peer.socket.infoHashes.indexOf(this.infoHash)
      arrayRemove(peer.socket.infoHashes, index)
    }

    peer._id || await this.peers.remove(id)
  }

  async _onAnnounceCompleted (params, peer, id) {
    if (!peer) {
      debug('unexpected `completed` event from peer that is not in swarm')
      return await this._onAnnounceStarted(params, peer, id) // treat as a start
    }
    if (peer.complete) {
      debug('unexpected `completed` event from peer that is already completed')
      return await this._onAnnounceUpdate(params, peer, id) // treat as an update
    }

    this.complete += 1
    this.incomplete -= 1
    peer.complete = true
    await this.peers.set(id, peer, { _: { peer: params } })
  }

  async _onAnnounceUpdate (params, peer, id) {
    if (!peer) {
      debug('unexpected `update` event from peer that is not in swarm')
      return await this._onAnnounceStarted(params, peer, id) // treat as a start
    }

    if (!peer.complete && params.left === 0) {
      this.complete += 1
      this.incomplete -= 1
      peer.complete = true
    }
    await this.peers.set(id, peer, { _: { peer: params } })
  }

  async _onAnnouncePaused (params, peer, id) {
    if (!peer) {
      debug('unexpected `paused` event from peer that is not in swarm')
      return await this._onAnnounceStarted(params, peer, id) // treat as a start
    }

    await this._onAnnounceUpdate(params, peer, id)
  }

  async _getPeers (numwant, ownPeerId, isWebRTC) {
    const peers = []
    const ite = randomIterate(Function.isFunction(this.peers.keys)
      ? await this.peers.keys() : this.peers.keys)
    let peerId
    while ((peerId = ite()) && peers.length < numwant) {
      // Don't mark the peer as most recently used on announce
      const peer = await this.peers.peek(peerId)
      if (!peer) continue
      if (isWebRTC && peer.peerId === ownPeerId) continue // don't send peer to itself
      if ((isWebRTC && peer.type !== 'ws') || (!isWebRTC && peer.type === 'ws')) continue // send proper peer type
      peers.push(peer)
    }
    return peers
  }
}

export default Swarm
