How Bitswap Works
=================

When a client requests blocks, Bitswap sends the CID of those blocks to its peers as "wants". When Bitswap receives a "want" from a peer, it responds with the corresponding block.

### Requesting Blocks

#### Sessions

Bitswap Sessions allow the client to make related requests to the same group of peers. For example typically requests to fetch all the blocks in a file would be made with a single session.

#### Discovery

To discover which peers have a block, Bitswap broadcasts a `want-have` message to all peers it is connected to asking if they have the block.

Any peers that have the block respond with a `HAVE` message. They are added to the Session.

If no connected peers have the block, Bitswap queries the DHT to find peers that have the block.

### Wants

When the client requests a block, Bitswap sends a `want-have` message with the block CID to all peers in the Session to ask who has the block.

Bitswap simultaneously sends a `want-block` message to one of the peers in the Session to request the block. If the peer does not have the block, it responds with a `DONT_HAVE` message. In that case Bitswap selects another peer and sends the `want-block` to that peer.

If no peers have the block, Bitswap broadcasts a `want-have` to all connected peers, and queries the DHT to find peers that have the block.

#### Peer Selection

Bitswap uses a probabilistic algorithm to select which peer to send `want-block` to, favouring peers that
- sent `HAVE` for the block
- were discovered as providers of the block in the DHT
- were first to send blocks to previous session requests

The selection algorithm includes some randomness so as to allow peers that are discovered later, but are more responsive, to rise in the ranking.

#### Periodic Search Widening

Periodically the Bitswap Session selects a random CID from the list of "pending wants" (wants that have been sent but for which no block has been received). Bitswap broadcasts a `want-have` to all connected peers and queries the DHT for the CID.

### Serving Blocks

#### Processing Requests

When Bitswap receives a `want-have` it checks if the block is in the local blockstore.

If the block is in the local blockstore Bitswap responds with `HAVE`. If the block is small Bitswap sends the block itself instead of `HAVE`.

If the block is not in the local blockstore, Bitswap checks the `send-dont-have` flag on the request. If `send-dont-have` is true, Bitswap sends `DONT_HAVE`. Otherwise it does not respond.

#### Processing Incoming Blocks

When Bitswap receives a block, it checks to see if any peers sent `want-have` or `want-block` for the block. If so it sends `HAVE` or the block itself to those peers.

#### Priority

Bitswap keeps requests from each peer in separate queues, ordered by the priority specified in the request message.

To select which peer to send the next response to, Bitswap chooses the peer with the least amount of data in its send queue. That way it will tend to "keep peers busy" by always keeping some data in each peer's send queue.


Implementation
==============

![Bitswap Components](./go-bitswap.png)

### Bitswap

The Bitswap class receives incoming messages and implements the Exchange API.

When a message is received, Bitswap
- Records some statistics about the message
- Informs the Engine of any new wants
  So that the Engine can send responses to the wants
- Informs the Engine of any received blocks
  So that the Engine can send the received blocks to any peers that want them
- Informs the SessionManager of received blocks, HAVEs and DONT_HAVEs
  So that the SessionManager can inform interested sessions

When the client makes an API call, Bitswap creates a new Session and calls the corresponding method (eg `GetBlocks()`).

### Sending Blocks

When the Engine is informed of new wants it
- Adds the wants to the Ledger (peer A wants block with CID Qmhash...)
- Checks the blockstore for the corresponding blocks, and adds a task to the PeerTaskQueue
  - If the blockstore does not have a wanted block, adds a `DONT_HAVE` task
  - If the blockstore has the block
    - for a `want-have` adds a `HAVE` task
    - for a `want-block` adds a `block` task

When the Engine is informed of new blocks it checks the Ledger to see if any peers want information about those blocks.
- For each block
  - For each peer that sent a `want-have` for the corresponding block
    Adds a `HAVE` task to the PeerTaskQueue
  - For each peer that sent a `want-block` for the corresponding block
    Adds a `block` task to the PeerTaskQueue

The Engine periodically pops tasks off the PeerTaskQueue, and creates a message with `blocks`, `HAVEs` and `DONT_HAVEs`.
The PeerTaskQueue prioritizes tasks such that the peers with the least amount of data in their send queue are highest priority, so as to "keep peers busy".

### Requesting Blocks

When the SessionManager is informed of a new message, it
- informs the BlockPresenceManager
  The BlockPresenceManager keeps track of which peers have sent HAVES and DONT_HAVEs for each block
- informs the Sessions that are interested in the received blocks and wants
- informs the PeerManager of received blocks
  The PeerManager checks if any wants were send to a peer for the received blocks. If so it sends a `CANCEL` message to those peers.

### Sessions

The Session starts in "discovery" mode. This means it doesn't have any peers yet, and needs to discover which peers have the blocks it wants.

When the client initially requests blocks from a Session, the Session
- informs the SessionInterestManager that it is interested in the want
- informs the sessionWantManager of the want
- tells the PeerManager to broadcast a `want-have` to all connected peers so as to discover which peers have the block
- queries the ProviderQueryManager to discover which peers have the block

When the session receives a message with `HAVE` or a `block`, it informs the SessionPeerManager. The SessionPeerManager keeps track of all peers in the session.
When the session receives a message with a `block` it informs the SessionInterestManager.

Once the session has peers it is no longer in "discovery" mode. When the client requests subsequent blocks the Session informs the sessionWantSender. The sessionWantSender tells the PeerManager to send `want-have` and `want-block` to peers in the session.

For each block that the Session wants, the sessionWantSender decides which peer is most likely to have a block by checking with the BlockPresenceManager which peers have sent a `HAVE` for the block. If no peers or multiple peers have sent `HAVE`, a peer is chosen probabilistically according to which how many times each peer was first to send a block in response to previous wants requested by the Session. The sessionWantSender sends a single "optimistic" `want-block` to the chosen peer, and sends `want-have` to all other peers in the Session.
When a peer responds with `DONT_HAVE`, the Session sends `want-block` to the next best peer, and so on until the block is received.

### PeerManager

The PeerManager creates a MessageQueue for each peer that connects to Bitswap. It remembers which `want-have` / `want-block` has been sent to each peer, and directs any new wants to the correct peer.
The MessageQueue groups together wants into a message, and sends the message to the peer. It monitors for timeouts and simulates a `DONT_HAVE` response if a peer takes too long to respond.

### Finding Providers

When bitswap can't find a connected peer who already has the block it wants, it falls back to querying a content routing system (a DHT in IPFS's case) to try to locate a peer with the block.

Bitswap routes these requests through the ProviderQueryManager system, which rate-limits these requests and also deduplicates in-process requests.

### Providing

As a bitswap client receives blocks, by default it announces them on the provided content routing system (again, a DHT in most cases). This behaviour can be disabled by passing `bitswap.ProvideEnabled(false)` as a parameter when initializing Bitswap. IPFS currently has its own experimental provider system ([go-ipfs-provider](https://github.com/ipfs/go-ipfs-provider)) which will eventually replace Bitswap's system entirely.

