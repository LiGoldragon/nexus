{
  description = "Nexus — the universal library and the serving every Nexus shares";

  inputs = {
    base.url = "github:LiGoldragon/build-base";
    # The generator that regenerates each committed Ethos generation for its check.
    ethos-zero = {
      url = "github:LiGoldragon/ethos-zero/2999292dc67c8608e54d32c333c550b29153cbff";
      inputs.base.follows = "base";
    };
  };

  # Every crate, third-party or member, is one buildRustCrate derivation.
  # Cargo.nix is generated from Cargo.lock by the build base's crate2nix and
  # committed, so evaluation imports no derivation. After editing a
  # Cargo.toml or Cargo.lock:
  #   nix run github:LiGoldragon/build-base#crate2nix -- generate
  outputs =
    { self, base, ethos-zero }:
    let
      lib = base.inputs.nixpkgs.lib;
      cargoNix = ./Cargo.nix;
      workspace = base.lib.workspaceOutputs {
        inherit cargoNix;
        # Each member's tests are written for every feature it declares on.
        memberChecks =
          { pkgs, workspace, check, ... }:
          lib.mapAttrs (
            name: member:
            let
              packageId = member.packageId;
              features = lib.attrNames (workspace.internal.crates.${packageId}.features or { });
              tests = (check name features).tests;
            in
            pkgs.runCommand "check-${name}" { passthru = { inherit tests; }; } ''
              cp -r ${tests.src} "$NIX_BUILD_TOP/source"
              chmod -R u+w "$NIX_BUILD_TOP/source"
              cd "$NIX_BUILD_TOP/source"
              mkdir -p "$(cat ${tests}/target-tmpdir)"
              for binary in ${tests}/tests/*; do
                echo "running $(basename "$binary")"
                "$binary"
              done
              touch $out
            ''
          ) workspace.workspaceMembers;
        extraChecks =
          { pkgs, toolchain, workspace, doc, ... }:
          let
            documentation = lib.listToAttrs (
              map (member: {
                name = "doc-${member}";
                value = doc member (lib.attrNames (workspace.internal.crates.${workspace.workspaceMembers.${member}.packageId}.features or { })) [ "-D" "warnings" ];
              }) (lib.attrNames workspace.workspaceMembers)
            );
            generator = ethos-zero.packages.${pkgs.stdenv.hostPlatform.system}.default;
            contract =
              { name, declaration, committed }:
              pkgs.runCommand "nexus-${name}-generated" {
                inherit generator declaration committed;
              } (builtins.readFile ./checks/generated-contract.sh);
          in
          documentation
          // {
            exchange-generated = contract {
              name = "exchange";
              declaration = ./crates/exchange/ethos/exchange.ethos;
              committed = ./crates/exchange/src/generated/exchange.rs;
            };
            serving-generated = contract {
              name = "serving";
              declaration = ./crates/serving/ethos/serving.ethos;
              committed = ./crates/serving/src/generated/serving.rs;
            };
            library-generated = contract {
              name = "library";
              declaration = ./crates/nexus/ethos/library.ethos;
              committed = ./crates/nexus/src/generated/library.rs;
            };
            fmt = pkgs.runCommand "nexus-fmt" { nativeBuildInputs = [ toolchain ]; } ''
              cd ${./.}
              find crates -name '*.rs' -not -path '*/generated/*' -print0 \
                | xargs -0 rustfmt --check --edition 2024
              touch "$out"
            '';
          };
      };
    in
    {
      inherit (workspace) packages checks devShells;
    };
}
