[![Crates.io](https://img.shields.io/crates/v/tokio-byteorder.svg)](https://crates.io/crates/tokio-byteorder)
[![Documentation](https://docs.rs/tokio-byteorder/badge.svg)](https://docs.rs/tokio-byteorder/)
[![Build Status](https://dev.azure.com/jonhoo/jonhoo/_apis/build/status/tokio-byteorder?branchName=master)](https://dev.azure.com/jonhoo/jonhoo/_build/latest?definitionId=12&branchName=master)
[![Codecov](https://codecov.io/github/jonhoo/tokio-byteorder/coverage.svg?branch=master)](https://codecov.io/gh/jonhoo/tokio-byteorder)
[![Dependency status](https://deps.rs/repo/github/jonhoo/tokio-byteorder/status.svg)](https://deps.rs/repo/github/jonhoo/tokio-byteorder)

This crate provides convenience methods for encoding and decoding numbers in
either [big-endian or little-endian order] on top of asynchronous I/O streams.
It owes everything to the magnificent [`byteorder`] crate. This crate only
provides a shim to [`AsyncRead`] and [`AsyncWrite`].

[big-endian or little-endian order]: https://en.wikipedia.org/wiki/Endianness
[`byteorder`]: https://github.com/BurntSushi/byteorder/
[`AsyncRead`]: https://docs.rs/tokio/0.2.0-alpha.4/tokio/io/trait.AsyncRead.html
[`AsyncWrite`]: https://docs.rs/tokio/0.2.0-alpha.4/tokio/io/trait.AsyncWrite.html
