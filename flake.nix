{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    rust-overlay.url = "github:oxalica/rust-overlay";
    # The local-noq package replaces this placeholder with a checkout at invocation time.
    noq-src = {
      url = "path:./moq_noq";
      flake = false;
    };
  };
  outputs =
    inputs@{
      nixpkgs,
      flake-parts,
      rust-overlay,
      noq-src,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      perSystem =
        {
          self',
          system,
          ...
        }:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ (import rust-overlay) ];
          };
          rust = pkgs.rust-bin.stable."1.95.0";
          rustToolchain = rust.minimal;
          devToolchain = rust.default.override {
            extensions = [
              "rust-src"
              "clippy"
              "rustfmt"
              "rust-analyzer"
            ];
            targets = [ "wasm32-unknown-unknown" ];
          };
          rustPlatform = pkgs.makeRustPlatform {
            cargo = rustToolchain;
            rustc = rustToolchain;
          };
          buildMember =
            path: localNoq:
            let
              manifest = (pkgs.lib.importTOML (./. + "/${path}/Cargo.toml")).package;
              lockFile = if localNoq then ./moq_cli/nixnet/noq.Cargo.lock else ./Cargo.lock;
            in
            rustPlatform.buildRustPackage {
              pname = manifest.name;
              version = manifest.version;
              cargoLock = {
                inherit lockFile;
                outputHashes = {
                  "octets-0.3.5" = "sha256-ZgcthLLe3XVBwz1neN89uhBfGWMkgfeNcvuUSTEi1XY=";
                  "quiche_endpoint-0.1.0" = "sha256-6Ge1+GcJVBeJBi3mbFExzVU4ow4CvhEmV3spJfoSeR4=";
                  "quiche_mio_runner-0.1.0" = "sha256-/DrryLuEyl82/JGZ4xVCAOrCAoBohe4nIXP7CMeK95s=";
                };
              };
              # Git-tracked files only (drops target/, .direnv/, drafts/, qlog/*.sqlog
              # etc. via .gitignore), minus tracked dirs/files that aren't part of the
              # Cargo build - keeps the src (and its hash) from churning on unrelated
              # changes, e.g. editing interop/ no longer invalidates every crate build.
              src = pkgs.lib.fileset.toSource {
                root = ./.;
                fileset = pkgs.lib.fileset.difference (pkgs.lib.fileset.gitTracked ./.) (
                  pkgs.lib.fileset.unions [
                    ./interop
                    ./.github
                    ./README.md
                    ./logo.svg
                    ./moq_cli/nixnet
                  ]
                );
              };
              buildAndTestSubdir = [ path ];
              cargoBuildFlags = pkgs.lib.optional localNoq "--locked";
              cargoTestFlags = pkgs.lib.optional localNoq "--locked";
              prePatch = pkgs.lib.optionalString localNoq ''
                if [ ! -f ${noq-src}/noq/Cargo.toml ]; then
                  echo 'Pass --override-input noq-src git+file:/path/to/noq (moq/noq-src from nixnet)' >&2
                  exit 1
                fi
                cp ${lockFile} Cargo.lock
                cat >> .cargo/config.toml <<'EOF'

                [patch.crates-io]
                noq = { path = "${noq-src}/noq" }
                EOF
              '';
              nativeBuildInputs = with pkgs; [
                clang
                git
                cmake
              ];
              env = {
                LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
                # Without this, panic!/#[track_caller] location strings embed
                # the real /nix/store path of both our own (vendored) source
                # and the rustc/rust-std sysroot, and Nix's reference scanner
                # then adds the *entire* ~2GB rust toolchain closure as a
                # runtime dependency of the built binary (verified: moq-cli's
                # closure was 2GB, 99.6% of it rustc/cargo/rust-std/etc. -
                # the actual binary is 8MB). Remapping both to fake,
                # non-store paths stops the scanner from seeing them.
                RUSTFLAGS = "--remap-path-prefix=/build=/src --remap-path-prefix=${rustToolchain}=/rust-toolchain";
              };
            };
          interopImages = import ./interop/interop.nix {
            inherit pkgs;
            moq-cli = self'.packages.moq-cli;
          };
        in
        {
          packages.moq-cli = buildMember "moq_cli" false;
          packages.moq-cli-local-noq = buildMember "moq_cli" true;
          packages.time-client-example = buildMember "quiche_moq/examples/time-client" false;
          packages.time-server-example = buildMember "quiche_moq/examples/time-server" false;
          packages.video-client-example = buildMember "quiche_moq/examples/video-client" false;
          packages.video-server-example = buildMember "quiche_moq/examples/video-server" false;
          packages.default = self'.packages.moq-cli;
          packages.interop-image = interopImages.image;
          devShells.default = pkgs.mkShell {
            inputsFrom = [
              self'.packages.default
            ];
            packages = [ devToolchain pkgs.ffmpeg pkgs.wasm-pack ];
            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
            RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
            shellHook = ''
              # Symlink for IDEs
              ln -sfn ${devToolchain} $PWD/.rust-toolchain
            '';
          };
        };
    };
}
