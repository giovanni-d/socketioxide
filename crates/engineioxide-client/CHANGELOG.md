# engineioxide-client (unreleased)
* feat: `Client::open_packet` returns the open packet of the handshake.
* feat: `Client::heartbeat_deadline` returns the instant after which the next poll yields a heartbeat
timeout. The stream checks the heartbeat only when polled, so a caller that arms a timer on this
instant detects a server that stopped pinging.
