package bitswap

import (
	"context"
	"fmt"

	engine "github.com/ipfs/go-bitswap/internal/decision"
	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	cid "github.com/ipfs/go-cid"
	process "github.com/jbenet/goprocess"
	procctx "github.com/jbenet/goprocess/context"
)

// TaskWorkerCount is the total number of simultaneous threads sending
// outgoing messages
var TaskWorkerCount = 8

func (bs *Bitswap) startWorkers(ctx context.Context, px process.Process) {

	// Start up workers to handle requests from other nodes for the data on this node
	for i := 0; i < TaskWorkerCount; i++ {
		i := i
		px.Go(func(px process.Process) {
			bs.taskWorker(ctx, i)
		})
	}

	if bs.provideEnabled {
		// Start up a worker to manage sending out provides messages
		px.Go(func(px process.Process) {
			bs.provideCollector(ctx)
		})

		// Spawn up multiple workers to handle incoming blocks
		// consider increasing number if providing blocks bottlenecks
		// file transfers
		px.Go(bs.provideWorker)
	}
}

func (bs *Bitswap) taskWorker(ctx context.Context, id int) {
	defer log.Debug("bitswap task worker shutting down...")
	log := log.With("ID", id)
	for {
		log.Debug("Bitswap.TaskWorker.Loop")
		select {
		case nextEnvelope := <-bs.engine.Outbox():
			select {
			case envelope, ok := <-nextEnvelope:
				if !ok {
					continue
				}

				// update the BS ledger to reflect sent message
				// TODO: Should only track *useful* messages in ledger
				outgoing := bsmsg.New(false)
				for _, block := range envelope.Message.Blocks() {
					log.Debugw("Bitswap.TaskWorker.Work",
						"Target", envelope.Peer,
						"Block", block.Cid(),
					)
					outgoing.AddBlock(block)
				}
				for _, blockPresence := range envelope.Message.BlockPresences() {
					outgoing.AddBlockPresence(blockPresence.Cid, blockPresence.Type)
				}
				// TODO: Only record message as sent if there was no error?
				bs.engine.MessageSent(envelope.Peer, outgoing)

				bs.sendBlocks(ctx, envelope)
				bs.counterLk.Lock()
				for _, block := range envelope.Message.Blocks() {
					bs.counters.blocksSent++
					bs.counters.dataSent += uint64(len(block.RawData()))
				}
				bs.counterLk.Unlock()
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (bs *Bitswap) sendBlocks(ctx context.Context, env *engine.Envelope) {
	// Blocks need to be sent synchronously to maintain proper backpressure
	// throughout the network stack
	defer env.Sent()

	msgSize := 0
	msg := bsmsg.New(false)

	for _, blockPresence := range env.Message.BlockPresences() {
		c := blockPresence.Cid
		switch blockPresence.Type {
		case pb.Message_Have:
			log.Infof("Sending HAVE %s to %s", c.String()[2:8], env.Peer)
		case pb.Message_DontHave:
			log.Infof("Sending DONT_HAVE %s to %s", c.String()[2:8], env.Peer)
		default:
			panic(fmt.Sprintf("unrecognized BlockPresence type %v", blockPresence.Type))
		}

		msgSize += bsmsg.BlockPresenceSize(c)
		msg.AddBlockPresence(c, blockPresence.Type)
	}
	for _, block := range env.Message.Blocks() {
		msgSize += len(block.RawData())
		msg.AddBlock(block)
		log.Infof("Sending block %s to %s", block, env.Peer)
	}

	bs.sentHistogram.Observe(float64(msgSize))
	err := bs.network.SendMessage(ctx, env.Peer, msg)
	if err != nil {
		// log.Infof("sendblock error: %s", err)
		log.Errorf("SendMessage error: %s. size: %d. block-presence length: %d", err, msg.Size(), len(env.Message.BlockPresences()))
	}
	log.Infof("Sent message to %s", env.Peer)
}

func (bs *Bitswap) provideWorker(px process.Process) {
	// FIXME: OnClosingContext returns a _custom_ context type.
	// Unfortunately, deriving a new cancelable context from this custom
	// type fires off a goroutine. To work around this, we create a single
	// cancelable context up-front and derive all sub-contexts from that.
	//
	// See: https://github.com/ipfs/go-ipfs/issues/5810
	ctx := procctx.OnClosingContext(px)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	limit := make(chan struct{}, provideWorkerMax)

	limitedGoProvide := func(k cid.Cid, wid int) {
		defer func() {
			// replace token when done
			<-limit
		}()

		log.Debugw("Bitswap.ProvideWorker.Start", "ID", wid, "cid", k)
		defer log.Debugw("Bitswap.ProvideWorker.End", "ID", wid, "cid", k)

		ctx, cancel := context.WithTimeout(ctx, provideTimeout) // timeout ctx
		defer cancel()

		if err := bs.network.Provide(ctx, k); err != nil {
			log.Warning(err)
		}
	}

	// worker spawner, reads from bs.provideKeys until it closes, spawning a
	// _ratelimited_ number of workers to handle each key.
	for wid := 2; ; wid++ {
		log.Debug("Bitswap.ProvideWorker.Loop")

		select {
		case <-px.Closing():
			return
		case k, ok := <-bs.provideKeys:
			if !ok {
				log.Debug("provideKeys channel closed")
				return
			}
			select {
			case <-px.Closing():
				return
			case limit <- struct{}{}:
				go limitedGoProvide(k, wid)
			}
		}
	}
}

func (bs *Bitswap) provideCollector(ctx context.Context) {
	defer close(bs.provideKeys)
	var toProvide []cid.Cid
	var nextKey cid.Cid
	var keysOut chan cid.Cid

	for {
		select {
		case blkey, ok := <-bs.newBlocks:
			if !ok {
				log.Debug("newBlocks channel closed")
				return
			}

			if keysOut == nil {
				nextKey = blkey
				keysOut = bs.provideKeys
			} else {
				toProvide = append(toProvide, blkey)
			}
		case keysOut <- nextKey:
			if len(toProvide) > 0 {
				nextKey = toProvide[0]
				toProvide = toProvide[1:]
			} else {
				keysOut = nil
			}
		case <-ctx.Done():
			return
		}
	}
}
