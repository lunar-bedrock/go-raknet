# RakNet transport and receive hardening

## Goal

Improve Linux packet throughput and flood resilience without adding batching delay, changing packet order within a connection, or importing Oomph's 100 ms ACK change. Deliver the work as three reviewable, stacked pull requests against `lunar`.

## PR 1: Linux batched socket I/O

- Add an internal packet-transport abstraction so normal `net.PacketConn` and custom listeners retain a portable one-packet fallback.
- On Linux UDP sockets, drain the datagrams already queued by the kernel with `recvmmsg`. The call does not wait to fill a batch and introduces no timer or flush interval.
- Process returned messages sequentially in kernel receive order. Use a bounded batch (initially 64 datagrams) to avoid monopolising the listener.
- Reuse receive buffers. Buffer ownership remains synchronous in this PR, so a datagram is not reused until its handler returns.
- Keep outbound writes as individual datagrams. `sendmmsg` is out of scope until profiling shows outbound syscall pressure.
- Add transport counters and benchmarks that expose datagrams per receive syscall and batch utilisation with negligible disabled-path cost.

## PR 2: Ordered receive isolation and flood controls

- Give each established connection a bounded single-consumer receive queue. This preserves packet order within that connection while preventing one connection's processing from blocking every other connection.
- Copy datagrams before transferring them to a connection queue, matching Oomph's simple channel-ownership model. Pooling and ownership API changes are out of scope. On queue overflow, drop according to an explicit policy and increment a counter; never reorder queued packets.
- Move unconnected handshake work to a bounded worker pool so connection attempts cannot block established traffic.
- Add conservative global and per-source pong rate limits. Valid discovery remains functional; excess discovery traffic is dropped and counted.
- Expose queue depth/high-water, overflow drops, unconnected queue drops, and pong-limit drops through cheap atomic statistics.
- Add no receive or send timer. Work begins as soon as a kernel batch is read, and each connection consumes packets immediately in order.

## PR 3: Congestion and parser hardening

- Rebase and review the existing `codex/security-congestion-master` work on top of PR 2 rather than importing Oomph's congestion implementation wholesale.
- Retain its tested sliding-window pacing, timeout/backoff behavior, bounded ACK/NACK expansion, resend budgets, split-packet limits, duplicate-fragment handling, and matched-pong RTT validation.
- Preserve protocol correctness and the existing ACK scheduling unless a focused test demonstrates a required change. In particular, do not port Oomph's additional 100 ms ACK delay.
- Re-run and extend regression, race, fuzz, and abuse tests against the new asynchronous receive ownership model.

## Ordering and latency guarantees

- A single socket's `recvmmsg` results are dispatched in their returned order.
- A connection has exactly one queue consumer, so packets for that connection are processed in arrival order.
- Batching is syscall amortisation only: it consumes packets already waiting in the kernel. There is no 50 ms or 100 ms collection window.
- Cross-connection execution order is intentionally unspecified; isolation allows independent clients to progress concurrently.
- Server-originated writes and flush policy remain outside these transport PRs. Dragonfly/Oomph's immediate response batching is a separate write-side concern.

## Verification

- Unit tests for fallback transport behavior, ordering, queue overflow, ownership, rate limits, and counters.
- Linux tests for batched receive order; build checks for Windows and unsupported platforms.
- `go test -race ./...`, targeted fuzz/regression runs, and benchmarks comparing single-datagram and batched receive paths.
- Each PR is opened ready for review and is not considered complete until checks pass and current review-bot findings are resolved or rebutted.

## Integration boundary

These PRs modify `HashimTheArab/go-raknet`. Updating Velvet, Oomph, or Dragonfly root modules to pin the merged fork is a separate follow-up because dependency-level `replace` directives do not propagate to consumers.
