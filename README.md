# Nexus

`nexus` is the universal library for Nexus components. It owns the standard
ontology shared by every long-running Nexus while component repositories own
their domain configuration, signal contracts, Sema records, and effects.

The first shared type is `ConfigurationState<C>`. A fresh Sema seeds it from
the executable's built-in default. Ordinary Configure may update it while no
meta Configure has occurred. A successful meta Configure closes ordinary
Configure, and only a meta reversal opens it again.

The stored configuration is desired state. A running Nexus keeps the startup
snapshot active and applies newly persisted socket settings on its next
zero-argument start. Its stable Sema discovery path is executable-owned rather
than mutable configuration, so configuration cannot redirect a Nexus away from
its domain state.
