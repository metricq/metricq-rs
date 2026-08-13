use protobuf_codegen::Codegen;

fn main() {
    Codegen::new()
        // All inputs and imports from the inputs must reside in `includes` directories.
        .includes(["lib/metricq-protobuf"])
        // Inputs must reside in some of include paths.
        .input("lib/metricq-protobuf/datachunk.proto")
        .input("lib/metricq-protobuf/history.proto")
        // Specify output directory relative to Cargo output directory.
        .cargo_out_dir("metricq-protobuf")
        .run_from_script();
}
