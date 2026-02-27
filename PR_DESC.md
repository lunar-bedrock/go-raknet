id.go - https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/channel/raknet/RakConstants.java#L80-L99

unconnected_status.go:
- decoding (offline statuses): https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/client/RakClientOfflineHandler.java#L128-L159
- encoding ID_ALREADY_CONNECTED: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/server/RakServerOfflineHandler.java#L286-L292
- encoding ID_CONNECTION_REQUEST_FAILED: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/server/RakServerOnlineInitialHandler.java#L113-L121

err.go:
- disconnect reasons: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/channel/raknet/RakDisconnectReason.java#L19-L30
- offline failure surfacing: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/client/RakClientOfflineHandler.java#L143-L158
- online failure surfacing: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/client/RakClientOnlineInitialHandler.java#L77-L84

listener.go:
- routing split between offline vs existing child channel: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/server/RakServerRouteHandler.java#L45-L50
- child channel lookup by sender address: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/channel/raknet/RakServerChannel.java#L101-L103
- max-connections option: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/channel/raknet/config/RakChannelOption.java#L67-L70
- max-connections config interface: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/channel/raknet/config/RakServerChannelConfig.java#L36-L39
- max-connections config impl: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/channel/raknet/config/DefaultRakServerConfig.java#L196-L203

conn.go:
- client GUID parsed from REQUEST_2 and passed into child: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/server/RakServerOfflineHandler.java#L239-L257
- client GUID stored in child/session config: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/channel/raknet/RakChildChannel.java#L50-L53

handler.go:
- offline REQUEST_2 handling (cookie, mtu checks, already connected, reply2): https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/server/RakServerOfflineHandler.java#L206-L275
- already-connected response packet: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/server/RakServerOfflineHandler.java#L286-L292
- online CONNECTION_REQUEST validation + fail/accept: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/server/RakServerOnlineInitialHandler.java#L82-L124
- existing-channel behavior when address already present: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/channel/raknet/RakServerChannel.java#L77-L86
- duplicate REQUEST_2 divergence reference (Cloudburst drops if pending is missing): https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/server/RakServerOfflineHandler.java#L213-L219
- cloudburst TODOs mirrored in our code:
  - https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/server/RakServerOfflineHandler.java#L173-L174
  - https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/server/RakServerOfflineHandler.java#L238

dial.go:
- offline status handling during handshake: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/client/RakClientOfflineHandler.java#L135-L159
- offline magic check before status handling: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/client/RakClientOfflineHandler.java#L128-L133
- online ID_CONNECTION_REQUEST_FAILED handling: https://github.com/CloudburstMC/Network/blob/048658bf815119f91a3bd23ae35a77a4c4073af8/transport-raknet/src/main/java/org/cloudburstmc/netty/handler/codec/raknet/client/RakClientOnlineInitialHandler.java#L77-L84
