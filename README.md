# yellowstone-jet-tpu-client

Minimal workspace for the TPU QUIC client and the per-slot live sender test.

## Build

```sh
cargo build -p yellowstone-jet-tpu-client --features examples
```

## Per-slot test

```sh
RPC_ENDPOINT=http://127.0.0.1:8899 \
GRPC_ENDPOINT=http://127.0.0.1:10000 \
GRPC_X_TOKEN='<token>' \
IDENTITY=/path/to/keypair.json \
cargo run -p yellowstone-jet-tpu-client --bin test-tpu-send-each-slot --features examples -- \
  --slots 100 --fast-wire
```

The fast-wire path sends full bincode-serialized transaction bytes directly to the current and
next leader TPU QUIC connections.
# Tpy-sender
