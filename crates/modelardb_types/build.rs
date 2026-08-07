use std::io::Result as StdIoResult;

fn main() -> StdIoResult<()> {
    // Tell Cargo to rerun the build script if the proto definition changes.
    println!("cargo::rerun-if-changed=src/flight/protocol.proto");

    prost_build::compile_protos(&["src/flight/protocol.proto"], &["src/"])?;
    Ok(())
}
