# RakNet transport hardening implementation plan

## PR 1: Linux batched socket I/O

1. Add failing tests for stable packet-control values, portable single-packet fallback, sequential batch dispatch, and transport counters.
2. Introduce an internal receive-message/transport interface without changing the public custom-listener contract.
3. Add Linux UDP batching with reusable buffers and ancillary-data parsing; retain portable fallback files for other platforms and custom packet connections.
4. Add failing dual-stack/interface-pinning tests, then route unconnected and established replies through the captured ingress control.
5. Add an opt-in reuse-port configuration with focused socket-option and fallback tests.
6. Add benchmarks for one-packet and queued-batch receive paths.
7. Run formatting, unit tests, race tests, cross-platform build tests, static analysis, and benchmarks.
8. Commit, push, open the ready-for-review PR against `lunar`, and resolve all current CI/review-bot findings.

## PR 2: Ordered receive isolation and flood controls

1. Create a stacked branch from PR 1.
2. Add failing tests proving per-connection order, non-blocking cross-connection progress, bounded overflow, and pooled-buffer ownership.
3. Implement one bounded single-consumer queue per connection and return owned receive buffers after handling.
4. Add failing tests for bounded unconnected work and global/per-source pong limiting.
5. Implement the bounded worker pool and token-bucket limits with atomic queue/drop statistics.
6. Add stress and allocation benchmarks covering slow consumers and discovery floods.
7. Run formatting, unit/race/stress tests, cross-platform builds, and static analysis.
8. Commit, push, open the stacked ready-for-review PR, and resolve all current CI/review-bot findings.

## PR 3: Congestion and parser hardening

1. Create a stacked branch from PR 2 and inventory every commit on `codex/security-congestion-master` by concern.
2. Reapply the parser, split, resend, ACK/NACK, RTT, and congestion changes in dependency order, resolving the new receive-ownership model deliberately.
3. Run each existing regression test at the point its production change is applied; add tests for any integration regression found.
4. Verify packet order, queue ownership, congestion pacing, timeouts, matched-pong RTT, parser bounds, and split allocation limits together.
5. Run the complete unit/race/fuzz/static-analysis suite and targeted benchmarks.
6. Commit, push, open the stacked ready-for-review PR, and resolve all current CI/review-bot findings.

## Follow-up after merges

Pin the merged `HashimTheArab/go-raknet` revision explicitly in the Velvet/Oomph/Dragonfly root module that owns the final dependency graph; a dependency's own `replace` directive is not inherited.
