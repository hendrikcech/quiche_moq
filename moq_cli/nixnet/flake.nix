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
          config = {
            name = "moq-quiche";
            nodePackages = with pkgs; [
              inputs'.moq.packages.moq-cli
              coreutils
              ffmpeg
            ];
            nodes = {
              client = {
                networking.interfaces.veth1.ipv4.addresses = [
                  {
                    address = "1.0.0.1";
                    prefixLength = 24;
                  }
                ];
                scripts.main = {
                  exec = ''
                    mkfifo /tmp/sv.mp4 /tmp/sa.mp4
                    moq --connect https://1.0.0.2 \
                      --sub "tos--video#out=/tmp/sv.mp4&filter=next-group-start" \
                      --sub "tos--audio#out=/tmp/sa.mp4&filter=next-group-start" &
                    trap 'kill "$!" 2>/dev/null || true' EXIT
                    timeout 30s ffmpeg -nostdin -v error -xerror -abort_on empty_output_stream \
                      -i /tmp/sv.mp4 -i /tmp/sa.mp4 -map 0:v -map 1:a -t 2 -f null -
                    echo "Decoded audio and video through the MoQ relay"
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
                };
                scripts.main = {
                  exec = ''
                    moq \
                    --listen 443 \
                    --connect 'https://2.0.0.2#upstream' \
                    --sub-passive "*" \
                    --pub-passive "*" \
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
                    moq --listen 443 \
                      --pub tos \
                      --pub-passive "tos--video#in=/tmp/pv.mp4" \
                      --pub-passive "tos--audio#in=/tmp/pa.mp4"
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
            };
          };
        in
        {
          packages.default = nixnet.mkExperiment config;
          packages.mermaid = nixnet.mkMermaid config;
          packages.mermaid-svg = nixnet.mkMermaidSvg config;
        };
    };
}
