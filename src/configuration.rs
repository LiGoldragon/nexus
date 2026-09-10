//! Standard configuration lifecycle shared by every Nexus.
//!
//! Persistence remains in the component's Sema, beside its typed
//! configuration record. This module owns the lifecycle rule applied inside
//! that Sema write boundary.

/// The persisted standard Nexus configuration metadata.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ConfigurationState<Configuration> {
    pub desired_configuration: Configuration,
    pub meta_configure_occurred: bool,
}

impl<Configuration> ConfigurationState<Configuration> {
    /// Seed a fresh Sema with the executable's built-in default.
    pub fn from_default(desired_configuration: Configuration) -> Self {
        Self {
            desired_configuration,
            meta_configure_occurred: false,
        }
    }
}

/// A refused standard configuration lifecycle transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConfigurationTransitionError {
    #[error("ordinary Configure is closed after meta Configure")]
    OrdinaryConfigureClosed,
}

/// Capability to apply the standard Nexus configuration transitions.
///
/// Ordinary Configure remains available until a meta Configure occurs. Only
/// the meta surface can close or reopen ordinary Configure.
pub trait Configurable<Configuration> {
    fn desired_configuration(&self) -> &Configuration;
    fn meta_configure_occurred(&self) -> bool;
    fn ordinary_configure_if_unset(
        &mut self,
        configuration: Configuration,
    ) -> Result<(), ConfigurationTransitionError>;
    fn meta_configure(&mut self, configuration: Configuration);
    fn meta_reverse(&mut self);
}

impl<Configuration> Configurable<Configuration> for ConfigurationState<Configuration> {
    fn desired_configuration(&self) -> &Configuration {
        &self.desired_configuration
    }

    fn meta_configure_occurred(&self) -> bool {
        self.meta_configure_occurred
    }

    fn ordinary_configure_if_unset(
        &mut self,
        configuration: Configuration,
    ) -> Result<(), ConfigurationTransitionError> {
        if self.meta_configure_occurred {
            return Err(ConfigurationTransitionError::OrdinaryConfigureClosed);
        }
        self.desired_configuration = configuration;
        Ok(())
    }

    fn meta_configure(&mut self, configuration: Configuration) {
        self.desired_configuration = configuration;
        self.meta_configure_occurred = true;
    }

    fn meta_reverse(&mut self) {
        self.meta_configure_occurred = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_configuration_closes_only_after_an_actual_meta_configure() {
        let mut state = ConfigurationState::from_default("default");
        state
            .ordinary_configure_if_unset("ordinary")
            .expect("ordinary Configure is open on a fresh Sema");
        assert_eq!(state.desired_configuration(), &"ordinary");
        assert!(!state.meta_configure_occurred());

        state.meta_configure("meta");
        assert_eq!(state.desired_configuration(), &"meta");
        assert!(state.meta_configure_occurred());
        assert_eq!(
            state.ordinary_configure_if_unset("refused"),
            Err(ConfigurationTransitionError::OrdinaryConfigureClosed)
        );
        assert_eq!(state.desired_configuration(), &"meta");
    }

    #[test]
    fn only_the_meta_reversal_reopens_ordinary_configuration() {
        let mut state = ConfigurationState::from_default("default");
        state.meta_configure("meta");
        state.meta_reverse();
        assert!(!state.meta_configure_occurred());
        state
            .ordinary_configure_if_unset("ordinary again")
            .expect("meta reversal reopens ordinary Configure");
        assert_eq!(state.desired_configuration(), &"ordinary again");
    }

    #[test]
    fn configuration_state_is_a_portable_archive() {
        let state = ConfigurationState::from_default(String::from("default"));
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&state).expect("archive state");
        let restored = rkyv::from_bytes::<ConfigurationState<String>, rkyv::rancor::Error>(&bytes)
            .expect("restore state");
        assert_eq!(restored, state);
    }
}
