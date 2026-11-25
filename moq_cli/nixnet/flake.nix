{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixnet.url = "github:birneee/nixnet";
    moq.url = "path:../..";
  };
  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      perSystem =
        { pkgs, inputs', ... }:
        let
          nixnet = inputs'.nixnet.legacyPackages;
          config = backend: moq: scheduler: let
            inherit (pkgs.lib) optional optionalAttrs optionalString;
            multipath = scheduler != null;
            edgeMoq = if multipath then "${inputs'.moq.packages.moq-cli-multipath}/bin/moq" else "moq";
            edgeFlags = optionalString multipath " --multipath --scheduler ${scheduler}";
          in {
            name = if multipath then "moq-mpquic-${scheduler}" else "moq-${backend}";
            nodePackages = with pkgs; [
              moq
              coreutils
              ffmpeg-headless
            ] ++ optional multipath pkgs.gnugrep;
            nodes = {
              client = {
                networking.interfaces = {
                  veth1.ipv4.addresses = [
                    {
                      address = "1.0.0.1";
                      prefixLength = 24;
                    }
                  ];
                } // optionalAttrs multipath {
                  veth3.ipv4.addresses = [
                    {
                      address = "3.0.0.1";
                      prefixLength = 24;
                    }
                  ];
                };
                scripts.main = {
                  exec = ''
                    mkfifo /tmp/sv.mp4 /tmp/sa.mp4
                    ${edgeMoq} --backend ${backend} ${optionalString (backend == "noq") "--noq-insecure"}${edgeFlags} \
                      --connect https://1.0.0.2${optionalString multipath " --bind 1.0.0.1:0 --additional-path 3.0.0.1:4443=3.0.0.2:443"} \
                      --cache-depth 128 \
                      --sub "tos--video#filter=next-group-start" \
                      --sub "tos--audio#filter=next-group-start" \
                      --out "tos--video#file=/tmp/sv.mp4" \
                      --out "tos--audio#file=/tmp/sa.mp4"${optionalString multipath " >/tmp/moq-subscriber.log 2>&1"} &
                    trap 'kill "$!" 2>/dev/null || true${optionalString multipath "; wait \"$!\" 2>/dev/null || true; cat /tmp/moq-subscriber.log"}' EXIT
                    timeout 30s ffmpeg -nostdin -v error -xerror -abort_on empty_output_stream \
                      -i /tmp/sv.mp4 -i /tmp/sa.mp4 -map 0:v -map 1:a -t 2 -f null -
                    ${optionalString multipath ''
                      grep -Eq 'MPQUIC scheduler: ${scheduler}$' /tmp/moq-subscriber.log || {
                        echo "Subscriber did not report scheduler ${scheduler}" >&2
                        exit 1
                      }
                      grep -Eq 'MPQUIC path [^ ]+ established: 3\.0\.0\.1:[0-9]+ -> 3\.0\.0\.2:443$' /tmp/moq-subscriber.log || {
                        echo "Subscriber did not validate the second path" >&2
                        exit 1
                      }
                    ''}echo "Decoded audio and video through the MoQ relay"
                  '';
                  await = true;
                };
              };
              relay = {
                networking.interfaces = {
                  veth1.ipv4.addresses = [
                    {
                      address = "1.0.0.2";
                      prefixLength = 24;
                    }
                  ];
                  veth2.ipv4.addresses = [
                    {
                      address = "2.0.0.1";
                      prefixLength = 24;
                    }
                  ];
                } // optionalAttrs multipath {
                  veth3.ipv4.addresses = [
                    {
                      address = "3.0.0.2";
                      prefixLength = 24;
                    }
                  ];
                };
                scripts.main = {
                  exec = ''
                    # The subscriber waits for the relay to advertise the publisher's namespace.
                    ${edgeMoq} --backend ${backend} ${optionalString (backend == "noq") "--noq-insecure"}${edgeFlags} \
                      --listen 443 \
                      --connect 'https://2.0.0.2#upstream' \
                      --relay-namespace "tos--*" \
                      --sub "*" \
                      --pub "*" \
                      --cache-max-obj 1048576 \
                      --cache-avg-obj 65536 \
                      --cache-depth 64
                  '';
                };
              };
              server = {
                networking.interfaces.veth2.ipv4.addresses = [
                  {
                    address = "2.0.0.2";
                    prefixLength = 24;
                  }
                ];
                scripts.main = {
                  exec = ''
                    mkfifo /tmp/pv.mp4 /tmp/pa.mp4
                    ffmpeg -nostdin -y -v error -xerror \
                      -re -f lavfi -i testsrc2=size=320x180:rate=25 \
                      -re -f lavfi -i sine=frequency=440 \
                      -map 0:v -c:v libx264 -preset ultrafast -g 25 -threads 1 \
                      -movflags empty_moov+default_base_moof+frag_keyframe -f mp4 /tmp/pv.mp4 \
                      -map 1:a -c:a aac -movflags empty_moov+default_base_moof \
                      -frag_duration 1000000 -f mp4 /tmp/pa.mp4 &
                    # Keep the group start and codec config when a whole fragment arrives at once.
                    moq --backend ${backend} --listen 443 \
                      --pub tos \
                      --cache-depth 128 \
                      --pub "tos--video#passive" \
                      --pub "tos--audio#passive" \
                      --in "tos--video#file=/tmp/pv.mp4" \
                      --in "tos--audio#file=/tmp/pa.mp4"
                  '';
                };
              };
            };
            veths = {
              veth1 = {
                netem.delayMs = 5;
                a.node = "client";
                b.node = "relay";
              };
              veth2 = {
                netem.delayMs = 5;
                a.node = "relay";
                b.node = "server";
              };
            } // optionalAttrs multipath {
              veth3 = {
                netem.delayMs = 25;
                a.node = "client";
                b.node = "relay";
              };
            };
          } // optionalAttrs multipath {
            workDir = null;
          };
        in
        {
          packages.default = nixnet.mkExperiment (config "quiche" inputs'.moq.packages.moq-cli null);
          packages.noq = nixnet.mkExperiment (config "noq" inputs'.moq.packages.moq-cli null);
          packages.noq-local = nixnet.mkExperiment (config "noq" inputs'.moq.packages.moq-cli-local-noq null);
          packages.mpquic-min-rtt = nixnet.mkExperiment (config "noq" inputs'.moq.packages.moq-cli "min-rtt");
          packages.mpquic-blest = nixnet.mkExperiment (config "noq" inputs'.moq.packages.moq-cli "blest");
          packages.mermaid = nixnet.mkMermaid (config "quiche" inputs'.moq.packages.moq-cli null);
          packages.mermaid-svg = nixnet.mkMermaidSvg (config "quiche" inputs'.moq.packages.moq-cli null);
        };
    };
}
