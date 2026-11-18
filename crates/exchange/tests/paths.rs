//! A Nexus's four socket paths: named after the Nexus, in its place's runtime
//! directory.

use std::path::Path;

use nexus_exchange::{ComponentName, ComponentTitle, Named, SocketPaths};
use shared_translator::placement::{Place, PlaceName, Runtime, State};

struct Probe;

impl Named for Probe {
    fn name() -> ComponentName {
        ComponentName("probe".into())
    }

    fn title() -> ComponentTitle {
        ComponentTitle("Probe Nexus".into())
    }
}

#[test]
fn the_socket_paths_are_named_after_the_nexus_in_its_runtime_directory() {
    let runtime = Path::new("/run/user/1000/nexus/probe");
    let place = Place {
        place_name: PlaceName("probe".into()),
        runtime: Runtime(runtime.to_string_lossy().into_owned()),
        state: State(runtime.to_string_lossy().into_owned()),
    };
    let paths = SocketPaths::<Probe>::from(&place);
    assert_eq!(paths.public, runtime.join("probe.sock"));
    assert_eq!(paths.meta, runtime.join("probe-meta.sock"));
    assert_eq!(paths.translator, runtime.join("probe-translator.sock"));
    assert_eq!(
        paths.meta_translator,
        runtime.join("probe-meta-translator.sock")
    );
}
