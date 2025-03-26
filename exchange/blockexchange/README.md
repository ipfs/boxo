BlockExchange
=============

> An implementation of the Exchange interface

# Background

The BlockExchange is an implementation of the Exchange interface aimed at
serving block requests in the most efficient way.

It manages wantlists, block queues, peer queues, optimistic broadcasts etc. in
order to minimize resource usage in the most demanding environments.

This implementation can use any "swap" implementation, such as
Bitswap or HTTPSwap.

# Formery known as Bitswap

The bulk of this code was previously named "bitswap" and was part of the
"bitswap protocol implementation". This was before HTTP block retrieval was
introduced. The reality is that the code here was mostly guided to support the
exchange of blocks using the bitswap multistream protocol and implement the
Bitswap spec. Since this is no longer the only way swapping blocks and since
the Bitswap spec itself is unrelated to how we manage queues, sessions and
all the work to organize the retrieval of blocks, things have been re-named
accordingly.

# Examples

See the [Bitswap Transfer Example](../../examples/bitswap-transfer).
