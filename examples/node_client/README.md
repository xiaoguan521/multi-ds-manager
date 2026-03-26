# Node.js gRPC Client Example

This example shows how to call `multi-ds-manager` through gRPC from Node.js.

## 1. Install dependencies

```bash
cd examples/node_client
npm install
```

## 2. Start the Rust service

```bash
cargo run -- --grpc
```

## 3. Run the client

```bash
cd examples/node_client
npm start
```

## Notes

- The script loads `../../proto/dynamic_ds.proto` dynamically, so no extra stub-generation step is required.
- The example uses the demo caller defaults from `config.example.yaml`:
  `bootstrap-client / bootstrap-secret`.
- Update `jgbh`, `sql`, and credentials if your local config differs.
