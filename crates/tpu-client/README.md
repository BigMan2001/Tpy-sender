# yellowstone-jet-tpu-client

Standalone Solana TPU QUIC client with Yellowstone gRPC slot and leader tracking.

The hot path for this fork is the direct fast sender:

```rust
let wire_txn = bincode::serialize(&transaction)?;
sender
    .send_wire_transaction_bytes_direct_fast(bytes::Bytes::from(wire_txn))
    .await?;
```

That sends full serialized transaction bytes directly to already-open current and next leader
TPU QUIC connections. It does not use the internal transaction channel.

## Live Test

```sh
RPC_ENDPOINT=http://127.0.0.1:8899 \
GRPC_ENDPOINT=http://127.0.0.1:10000 \
GRPC_X_TOKEN='<token>' \
IDENTITY=/path/to/keypair.json \
cargo run -p yellowstone-jet-tpu-client --bin test-tpu-send-each-slot --features examples -- \
  --slots 100 --fast-wire
```
