//! Configuration and socket resolution, with explicit environment values so
//! tests and callers can resolve a place without changing process globals.
use crate::{Named, TextSocket, Unsent};
use datom_codec::{Actualizing, Budget, Potential};
use std::{
    ffi::OsString,
    path::{Component, Path, PathBuf},
};

#[derive(Clone, Debug, PartialEq, Eq, datom_codec::Composing)]
pub struct PlaceName(pub String);

#[derive(Clone, Debug, PartialEq, Eq, datom_codec::Composing)]
pub struct CliConfiguration {
    pub place_name: PlaceName,
}

/// The three standard variables read by a client, never by a Nexus.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CliEnvironment {
    pub configuration_home: Option<OsString>,
    pub home: Option<OsString>,
    pub runtime_directory: Option<OsString>,
}

pub trait EnvironmentReading {
    fn of_process() -> Self;
}

pub trait ConfigurationReading {
    fn read(&self) -> Result<CliConfiguration, Unsent>;
}

/// A component's shared ordinary/meta CLI configuration file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigurationFile(pub PathBuf);

pub trait SocketResolving {
    fn configuration_file<N: Named>(&self) -> Result<ConfigurationFile, Unsent>;
    fn translator_socket<N: Named>(&self, socket: TextSocket) -> Result<PathBuf, Unsent>;
}

impl EnvironmentReading for CliEnvironment {
    fn of_process() -> Self {
        Self {
            configuration_home: std::env::var_os("XDG_CONFIG_HOME"),
            home: std::env::var_os("HOME"),
            runtime_directory: std::env::var_os("XDG_RUNTIME_DIR"),
        }
    }
}

impl ConfigurationReading for ConfigurationFile {
    fn read(&self) -> Result<CliConfiguration, Unsent> {
        let path = self.0.to_string_lossy().into_owned();
        let source = std::fs::read_to_string(&self.0).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Unsent::Missing(path.clone())
            } else {
                Unsent::Unreadable {
                    path: path.clone(),
                    reason: error.to_string(),
                }
            }
        })?;
        // The flat configuration has a struct and one string. Its text length
        // bounds the reader work; its type fixes composition depth.
        let remaining = i64::try_from(source.len())
            .unwrap_or(i64::MAX)
            .saturating_add(1);
        let mut budget = Budget {
            remaining,
            reader: protos::ReaderBudget { remaining },
            depth: 0,
            maximum_depth: 2,
        };
        let configuration = Potential::<CliConfiguration>::from(source)
            .actualize(&mut budget)
            .map_err(|error| Unsent::Invalid {
                path: path.clone(),
                reason: format!("{error:?}"),
            })?;
        let name = &configuration.place_name.0;
        let mut components = Path::new(name).components();
        if name.is_empty()
            || name.contains('/')
            || name.contains('\0')
            || !matches!(components.next(), Some(Component::Normal(_)))
            || components.next().is_some()
        {
            return Err(Unsent::Invalid {
                path,
                reason: "the place name must be one normal path component".into(),
            });
        }
        Ok(configuration)
    }
}

impl SocketResolving for CliEnvironment {
    fn configuration_file<N: Named>(&self) -> Result<ConfigurationFile, Unsent> {
        let file_name = format!("{}-nexus.datom", N::name());
        let root = match self
            .configuration_home
            .as_ref()
            .filter(|value| !value.is_empty())
        {
            Some(directory) => PathBuf::from(directory),
            None => match self.home.as_ref().filter(|value| !value.is_empty()) {
                Some(home) => PathBuf::from(home).join(".config"),
                None => {
                    return Err(Unsent::Invalid {
                        path: format!("~/.config/nexus/{file_name}"),
                        reason: "XDG_CONFIG_HOME and HOME are unset".into(),
                    });
                }
            },
        };
        let path = root.join("nexus").join(file_name);
        if !root.is_absolute() {
            return Err(Unsent::Invalid {
                path: path.to_string_lossy().into_owned(),
                reason: "the configuration root must be absolute".into(),
            });
        }
        Ok(ConfigurationFile(path))
    }

    fn translator_socket<N: Named>(&self, socket: TextSocket) -> Result<PathBuf, Unsent> {
        let file = self.configuration_file::<N>()?;
        let configuration = file.read()?;
        let runtime = self
            .runtime_directory
            .as_ref()
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .ok_or_else(|| Unsent::Invalid {
                path: file.0.to_string_lossy().into_owned(),
                reason: "XDG_RUNTIME_DIR must name an absolute directory".into(),
            })?;
        let socket_name = match socket {
            TextSocket::Ordinary => format!("{}-translator.sock", N::name()),
            TextSocket::Meta => format!("{}-meta-translator.sock", N::name()),
        };
        Ok(runtime
            .join("nexus")
            .join(configuration.place_name.0)
            .join(socket_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Example;
    impl Named for Example {
        fn name() -> &'static str {
            "example"
        }
    }

    #[test]
    fn configuration_roots_and_empty_xdg_follow_the_standard() {
        let mut environment = CliEnvironment {
            home: Some("/home/example".into()),
            ..CliEnvironment::default()
        };
        for configuration_home in [None, Some("".into())] {
            environment.configuration_home = configuration_home;
            assert_eq!(
                environment.configuration_file::<Example>().unwrap().0,
                Path::new("/home/example/.config/nexus/example-nexus.datom")
            );
        }
        environment.configuration_home = Some("/configuration".into());
        assert_eq!(
            environment.configuration_file::<Example>().unwrap().0,
            Path::new("/configuration/nexus/example-nexus.datom")
        );
        environment.configuration_home = Some("relative".into());
        assert!(
            matches!(environment.configuration_file::<Example>(), Err(Unsent::Invalid { path, .. }) if path == "relative/nexus/example-nexus.datom")
        );
    }

    #[test]
    fn configuration_failures_name_the_file_and_valid_places_resolve_both_sockets() {
        let scratch = tempfile::tempdir().unwrap();
        std::fs::create_dir(scratch.path().join("nexus")).unwrap();
        let environment = CliEnvironment {
            configuration_home: Some(scratch.path().into()),
            runtime_directory: Some("/runtime".into()),
            home: None,
        };
        let file = environment.configuration_file::<Example>().unwrap();
        let path = file.0.to_string_lossy().into_owned();
        assert_eq!(file.read(), Err(Unsent::Missing(path.clone())));
        for text in ["{ next }", "{ «next» }\n"] {
            std::fs::write(&file.0, text).unwrap();
            assert_eq!(file.read().unwrap().place_name, PlaceName("next".into()));
            assert_eq!(
                environment
                    .translator_socket::<Example>(TextSocket::Ordinary)
                    .unwrap(),
                Path::new("/runtime/nexus/next/example-translator.sock")
            );
            assert_eq!(
                environment
                    .translator_socket::<Example>(TextSocket::Meta)
                    .unwrap(),
                Path::new("/runtime/nexus/next/example-meta-translator.sock")
            );
        }
        for text in [
            "",
            "{}",
            "{ next extra }",
            "{ .. }",
            "{ . }",
            "{ «» }",
            "{ a/b }",
            "{ /absolute }",
            "{ «nul\0place» }",
        ] {
            std::fs::write(&file.0, text).unwrap();
            assert!(
                matches!(file.read(), Err(Unsent::Invalid { path: named, .. }) if named == path),
                "{text:?}"
            );
        }
        std::fs::remove_file(&file.0).unwrap();
        std::fs::create_dir(&file.0).unwrap();
        assert!(
            matches!(file.read(), Err(Unsent::Unreadable { path: named, .. }) if named == path)
        );
    }

    #[test]
    fn missing_runtime_is_invalid_configuration_not_an_invented_place() {
        let scratch = tempfile::tempdir().unwrap();
        std::fs::create_dir(scratch.path().join("nexus")).unwrap();
        std::fs::write(scratch.path().join("nexus/example-nexus.datom"), "{ main }").unwrap();
        for runtime_directory in [None, Some("".into()), Some("relative".into())] {
            let environment = CliEnvironment {
                configuration_home: Some(scratch.path().into()),
                home: None,
                runtime_directory,
            };
            assert!(
                matches!(environment.translator_socket::<Example>(TextSocket::Ordinary),
                Err(Unsent::Invalid { path, .. }) if path.ends_with("example-nexus.datom"))
            );
        }
    }
}
