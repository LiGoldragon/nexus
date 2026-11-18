use ethos_zero::{Actualizing, File, Generating, Potential};
fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    let source = std::fs::read_to_string(root.join("ethos/library.ethos")).expect("source");
    let file = Potential::<File>::from(source)
        .actualize()
        .unwrap_or_else(|_| panic!("read"));
    let generated = file.generate().unwrap_or_else(|_| panic!("generate"));
    assert_eq!(
        generated,
        std::fs::read_to_string(root.join("src/generated/library.rs")).expect("generated")
    );
}
