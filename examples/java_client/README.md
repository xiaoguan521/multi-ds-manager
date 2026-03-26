# Java gRPC Client Example

This example shows how to call `multi-ds-manager` through gRPC from Java.

## 1. Start the Rust service

```bash
cargo run -- --grpc
```

## 2. Run the Java client

```bash
cd examples/java_client
mvn compile exec:java
```

## Notes

- The Maven build compiles `../../proto/dynamic_ds.proto` and generates both protobuf and gRPC stubs automatically.
- The example uses the demo caller defaults from `config.example.yaml`:
  `bootstrap-client / bootstrap-secret`.
- Update `jgbh`, `sql`, and credentials if your local config differs.
