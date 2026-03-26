fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    std::env::set_var("PROTOC", protoc);

    println!("cargo:rerun-if-changed=proto/dynamic_ds.proto");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("dynamic_ds_descriptor.bin"))
        .compile_protos(&["proto/dynamic_ds.proto"], &["proto"])?;

    Ok(())
}
