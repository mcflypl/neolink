# Changelog

All notable changes to this fork are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions up to and including `0.6.3-rc.3` were released by
[QuantumEntangledAndy/neolink](https://github.com/QuantumEntangledAndy/neolink);
their history lives in that repository.

## [Unreleased]

## [0.7.0] - 2026-10-15

First release of this fork, based on upstream commit `6e05e78` (`0.6.3-rc.3`).

### Fixed

- The RTSP server no longer freezes permanently when a camera stops answering
  while a client is connecting. Setting up a client stream (camera wake-up,
  stream info and the first frames) is now limited to 30 seconds, so the GLib
  main loop that serves every RTSP client can no longer block forever. Seen with
  4G cameras (Reolink Go PT Ultra) connected through the Reolink relay.
- Errors while setting up a client stream are now logged as warnings instead of
  being silently dropped.
- A stream whose RTSP client gave up during setup is no longer pumped into
  GStreamer, so it no longer keeps a battery camera awake.

### Changed

- Docker images are published to the GitHub Container Registry
  (`ghcr.io/mcflypl/neolink`) with the built-in `GITHUB_TOKEN` instead of
  Docker Hub.
- CI uses current versions of the GitHub and Docker actions and the
  `ubuntu-24.04`, `windows-2025` and `macos-15` runners; cross builds use Rust 1.99.0.
- README describes the fork and points to its releases and images; installation
  covers binaries and Docker together, and stale notes and typos are cleaned up.
- Dependencies updated to their latest semver-compatible releases; the old
  `rustix` 0.37 no longer breaks nightly Clippy.
- Windows binaries are built with MSYS2 MinGW64 (`x86_64-pc-windows-gnu`) and
  need GStreamer from MSYS2, because gstreamer.freedesktop.org no longer serves
  its Windows installers reliably. macOS binaries link against Homebrew's
  GStreamer and OpenSSL 3.
- pre-commit uses `pre-commit-hooks` v6.0.0 and runs `cargo fmt` as a local hook
  instead of the unmaintained `doublify/pre-commit-rust`.

### Removed

- macOS Intel build, as the `macos-12` runner it used is retired.
- Docker Hub image pruning workflow.
- Donation links and `FUNDING.yml`.
- markdownlint pre-commit hook and its configuration.

[Unreleased]: https://github.com/mcflypl/neolink/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/mcflypl/neolink/compare/6e05e78...v0.7.0
