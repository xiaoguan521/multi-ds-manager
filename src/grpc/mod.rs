pub mod server;

pub mod proto {
    tonic::include_proto!("multi_ds.grpc.v1");
}

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("dynamic_ds_descriptor");
