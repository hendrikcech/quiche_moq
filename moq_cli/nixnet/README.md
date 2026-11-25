# Two-path MPQUIC smoke test

From the repository root on Linux with network namespace support, point Nix at
a checkout of the **noq scheduler fork** (with the MinRTT and BLEST APIs):

```sh
noq_src="git+file:$(realpath ../noq)"
nix run ./moq_cli/nixnet#mpquic-min-rtt --override-input moq/noq-src "$noq_src"
nix run ./moq_cli/nixnet#mpquic-blest --override-input moq/noq-src "$noq_src"
```

Only the subscriber and relay use the multipath-enabled binary. The publisher
runs standard noq, so the relay-to-publisher hop remains single-path.

- Primary subscriber-to-relay link: `1.0.0.1` to `1.0.0.2`, 5 ms netem delay per direction.
- Additional subscriber-to-relay link: `3.0.0.1:4443` to `3.0.0.2:443`, 25 ms per direction.
- Relay-to-publisher link: `2.0.0.1` to `2.0.0.2`, 5 ms per direction.

The run fails unless FFmpeg decodes both tracks and the subscriber logs the
requested scheduler and successful validation of the second path. That proves
path establishment, not that both paths carried media or that one scheduler
outperformed the other. The MPQUIC runs log to the terminal and do not write to
`out/`. The default, `#noq`, and `#noq-local` runs do not enable multipath.
