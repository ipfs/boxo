# Bitswap Broadcast Reduction

The Bitswap client now supports broadcast reduction logic. This feature significantly reduces the number of broadcast messages sent to peers, resulting in lower bandwidth usage during load spikes.

The overall logic works by sending to non-local peers only if those peers have previously replied that they have data blocks. To minimize impact on existing workloads, by default, broadcasts are still always sent to peers on the local network, or the ones defined in `Peering.Peers`.


## Testing with Kubo

At Shipyard, we conducted A/B testing on our internal Kubo staging gateway with organic CID requests to `ipfs.io`. While these results may not exactly match your specific workload, the benefits proved significant enough to make this feature default. Here are the key findings:

- **Dramatic Resource Usage Reduction:** Internal testing demonstrated a reduction in Bitswap broadcast messages by 80-98% and network bandwidth savings of 50-95%, with the greatest improvements occurring during high traffic and peer spikes. These efficiency gains lower operational costs of running Kubo under high load and improve the IPFS Mainnet (which is >80% Kubo-based) by reducing ambient traffic for all connected peers.
- **Improved Memory Stability:** Memory stays stable even during major CID request spikes that increase peer count, preventing the out-of-memory (OOM) issues found in earlier Kubo versions.
- **Data Retrieval Performance Remains Strong:** Our tests suggest that Kubo gateway hosts with broadcast reduction enabled achieve similar or better HTTP 200 success rates compared to version 0.35, while maintaining equivalent or higher want-have responses and unique blocks received.

For more information about our A/B tests, see [kubo#10825](https://github.com/ipfs/kubo/pull/10825).


## Configuration Options in Boxo

- [BroadcastControlEnable](https://pkg.go.dev/github.com/ipfs/boxo/bitswap/client#BroadcastControlEnable): enables or disables broadcast reduction logic. Setting this to false restores the previous broadcast behavior of sending broadcasts to all peers, and ignores all other BroadcastControl options. Default is false (disabled).
- [BroadcastControlLocalPeers](https://pkg.go.dev/github.com/ipfs/boxo/bitswap/client#BroadcastControlLocalPeers): enables or disables broadcast control for peers on the local network. If false, than always broadcast to peers on the local network. If true, apply broadcast control to local peers. Default is false (always broadcast to local peers).
- [BroadcastControlMaxPeers](https://pkg.go.dev/github.com/ipfs/boxo/bitswap/client#BroadcastControlMaxPeers): sets a hard limit on the number of peers to send broadcasts to. A value of 0 means no broadcasts are sent. A value of -1 means there is no limit. Default is -1 (unlimited).
- [BroadcastControlMaxRandomPeers](https://pkg.go.dev/github.com/ipfs/boxo/bitswap/client#BroadcastControlMaxRandomPeers): sets the number of peers to broadcast to anyway, even though broadcast control logic has determined that they are not broadcast targets. Setting this to a non-zero value ensures at least this number of random peers receives a broadcast. This may be helpful in cases where peers that are not receiving broadcasts may have wanted blocks. Default is 0 (no random broadcasts).
- [BroadcastControlPeeredPeers](https://pkg.go.dev/github.com/ipfs/boxo/bitswap/client#BroadcastControlPeeredPeers): enables or disables broadcast control for peers configured for peering. If false, than always broadcast to peers configured for peering. If true, apply broadcast control to peered peers. Default is false (always broadcast to peered peers).
- [BroadcastControlSendToPendingPeers](https://pkg.go.dev/github.com/ipfs/boxo/bitswap/client#BroadcastControlSendToPendingPeers): enables or disables sending broadcasts to any peers to which there is a pending message to send. When enabled, this sends broadcasts to many more peers, but does so in a way that does not increase the number of separate broadcast messages. There is still the increased cost of the recipients having to process and respond to the broadcasts. Default is false.
