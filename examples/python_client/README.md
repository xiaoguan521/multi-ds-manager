# Python gRPC Client Example

This example shows how to call `multi-ds-manager` through gRPC from Python.

## 1. Install dependencies

```bash
python -m pip install -r examples/python_client/requirements.txt
```

## 2. Generate Python stubs

```bash
python -m grpc_tools.protoc ^
  -I proto ^
  --python_out=examples/python_client ^
  --grpc_python_out=examples/python_client ^
  proto/dynamic_ds.proto
```

## 3. Start the Rust service

```bash
cargo run -- --grpc
```

## 4. Run the example client

```bash
python examples/python_client/client.py
```

## Notes

- The example uses the demo caller defaults from `config.example.yaml`:
  `bootstrap-client / bootstrap-secret`.
- Update `jgbh`, `sql`, and credentials if your local config differs.
- The generated `dynamic_ds_pb2.py` and `dynamic_ds_pb2_grpc.py` files stay in this folder.
