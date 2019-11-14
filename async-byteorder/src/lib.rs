/*!
This crate provides convenience methods for encoding and decoding numbers in
either [big-endian or little-endian order] on top of asynchronous I/O streams.
It owes everything to the magnificent [`byteorder`] crate. This crate only
provides a shim to [`AsyncRead`] and [`AsyncWrite`].

The organization of the crate mirrors that of `byteorder`. A trait, [`ByteOrder`], specifies
byte conversion methods for each type of number in Rust (sans numbers that have
a platform dependent size like `usize` and `isize`). Two types, [`BigEndian`]
and [`LittleEndian`] implement these methods. Finally, [`AsyncReadBytesExt`] and
[`AsyncWriteBytesExt`] provide convenience methods available to all types that
implement [`Read`] and [`Write`].

An alias, [`NetworkEndian`], for [`BigEndian`] is provided to help improve
code clarity.

An additional alias, [`NativeEndian`], is provided for the endianness of the
local platform. This is convenient when serializing data for use and
conversions are not desired.

# Examples

Read unsigned 16 bit big-endian integers from an [`AsyncRead`] type:

```rust
use async_std::io::Cursor;
use async_byteorder::{BigEndian, AsyncReadBytesExt};

#[async_std::main]
async fn main() {
    let mut rdr = Cursor::new(vec![2, 5, 3, 0]);
    // Note that we use type parameters to indicate which kind of byte order
    // we want!
    assert_eq!(517, rdr.read_u16::<BigEndian>().await.unwrap());
    assert_eq!(768, rdr.read_u16::<BigEndian>().await.unwrap());
}
```

Write unsigned 16 bit little-endian integers to a [`AsyncWrite`] type:

```rust
use async_byteorder::{LittleEndian, AsyncWriteBytesExt};

#[async_std::main]
async fn main() {
    let mut wtr = vec![];
    wtr.write_u16::<LittleEndian>(517).await.unwrap();
    wtr.write_u16::<LittleEndian>(768).await.unwrap();
    assert_eq!(wtr, vec![5, 2, 0, 3]);
}
```

# Alternatives

Note that as of Rust 1.32, the standard numeric types provide built-in methods
like `to_le_bytes` and `from_le_bytes`, which support some of the same use
cases.

[big-endian or little-endian order]: https://en.wikipedia.org/wiki/Endianness
[`byteorder`]: https://github.com/BurntSushi/byteorder/
[`ByteOrder`]: trait.ByteOrder.html
[`BigEndian`]: enum.BigEndian.html
[`LittleEndian`]: enum.LittleEndian.html
[`AsyncReadBytesExt`]: trait.AsyncReadBytesExt.html
[`AsyncWriteBytesExt`]: trait.AsyncWriteBytesExt.html
[`NetworkEndian`]: type.NetworkEndian.html
[`NativeEndian`]: type.NativeEndian.html
[`AsyncRead`]: https://docs.rs/tokio/0.2.0-alpha.4/tokio/io/trait.AsyncRead.html
[`AsyncWrite`]: https://docs.rs/tokio/0.2.0-alpha.4/tokio/io/trait.AsyncWrite.html
*/

#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

use byteorder::ByteOrder;
use core::future::Future;
use core::marker::{PhantomData, Unpin};
use core::mem::size_of;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_io as io;

pub use byteorder::{BigEndian, LittleEndian, NativeEndian, NetworkEndian};

macro_rules! reader {
    ($name:ident, $ty:ty, $reader:ident) => {
        reader!($name, $ty, $reader, size_of::<$ty>());
    };
    ($name:ident, $ty:ty, $reader:ident, $bytes:expr) => {
        #[doc(hidden)]
        pub struct $name<R, T> {
            buf: [u8; $bytes],
            read: u8,
            src: R,
            bo: PhantomData<T>,
        }

        impl<R, T> $name<R, T> {
            fn new(r: R) -> Self {
                $name {
                    buf: [0; $bytes],
                    read: 0,
                    src: r,
                    bo: PhantomData,
                }
            }
        }

        impl<R, T> Future for $name<R, T>
        where
            R: io::AsyncRead,
            T: ByteOrder,
        {
            type Output = io::Result<$ty>;
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                if self.read == $bytes as u8 {
                    return Poll::Ready(Ok(T::$reader(&self.buf[..])));
                }

                // we need this so that we can mutably borrow multiple fields
                // it is safe as long as we never take &mut to src (since it has been pinned)
                // unless it is to place it in a Pin itself like below.
                let mut this = unsafe { self.get_unchecked_mut() };
                let mut src = unsafe { Pin::new_unchecked(&mut this.src) };

                while this.read < $bytes as u8 {
                    this.read += match src
                        .as_mut()
                        .poll_read(cx, &mut this.buf[this.read as usize..])
                    {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
                        Poll::Ready(Ok(0)) => {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "failed to fill whole buffer",
                            )));
                        }
                        Poll::Ready(Ok(n)) => n as u8,
                    };
                }
                Poll::Ready(Ok(T::$reader(&this.buf[..])))
            }
        }
    };
}

macro_rules! reader8 {
    ($name:ident, $ty:ty) => {
        #[doc(hidden)]
        pub struct $name<R>(R);
        impl<R> Future for $name<R>
        where
            R: io::AsyncRead,
        {
            type Output = io::Result<$ty>;
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let src = unsafe { self.map_unchecked_mut(|t| &mut t.0) };
                let mut buf = [0; 1];
                match src.poll_read(cx, &mut buf[..]) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
                    Poll::Ready(Ok(0)) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer",
                    ))),
                    Poll::Ready(Ok(1)) => Poll::Ready(Ok(buf[0] as $ty)),
                    Poll::Ready(Ok(_)) => unreachable!(),
                }
            }
        }
    };
}

reader8!(ReadU8, u8);
reader8!(ReadI8, i8);

reader!(ReadF32, f32, read_f32);
reader!(ReadF64, f64, read_f64);
reader!(ReadU16, u16, read_u16);
reader!(ReadU24, u32, read_u24, 3);
reader!(ReadU32, u32, read_u32);
reader!(ReadU48, u64, read_u48, 6);
reader!(ReadU64, u64, read_u64);
reader!(ReadU128, u128, read_u128);
reader!(ReadI16, i16, read_i16);
reader!(ReadI24, i32, read_i24, 3);
reader!(ReadI32, i32, read_i32);
reader!(ReadI48, i64, read_i48, 6);
reader!(ReadI64, i64, read_i64);
reader!(ReadI128, i128, read_i128);

macro_rules! read_impl {
    (
        $(#[$outer:meta])*
        fn $name:ident(&mut self) -> $($fut:tt)*
    ) => {
        $(#[$outer])*
        #[inline]
        fn $name<'a, T: ByteOrder>(&'a mut self) -> $($fut)*<&'a mut Self, T> where Self: Unpin {
            $($fut)*::new(self)
        }
    }
}

/// Extends [`AsyncRead`] with methods for reading numbers.
///
/// Most of the methods defined here have an unconstrained type parameter that
/// must be explicitly instantiated. Typically, it is instantiated with either
/// the [`BigEndian`] or [`LittleEndian`] types defined in this crate.
///
/// # Examples
///
/// Read unsigned 16 bit big-endian integers from a [`Read`]:
///
/// ```rust
/// use async_std::io::Cursor;
/// use async_byteorder::{BigEndian, AsyncReadBytesExt};
///
/// #[async_std::main]
/// async fn main() {
///     let mut rdr = Cursor::new(vec![2, 5, 3, 0]);
///     assert_eq!(517, rdr.read_u16::<BigEndian>().await.unwrap());
///     assert_eq!(768, rdr.read_u16::<BigEndian>().await.unwrap());
/// }
/// ```
///
/// [`BigEndian`]: enum.BigEndian.html
/// [`LittleEndian`]: enum.LittleEndian.html
/// [`AsyncRead`]: https://docs.rs/tokio/0.2.0-alpha.4/tokio/io/trait.AsyncRead.html
pub trait AsyncReadBytesExt: io::AsyncRead {
    /// Reads an unsigned 8 bit integer from the underlying reader.
    ///
    /// Note that since this reads a single byte, no byte order conversions
    /// are used. It is included for completeness.
    ///
    /// # Errors
    ///
    /// This method returns the same errors as [`Read::read_exact`].
    ///
    /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
    ///
    /// # Examples
    ///
    /// Read unsigned 8 bit integers from a `Read`:
    ///
    /// ```rust
    /// use async_std::io::Cursor;
    /// use async_byteorder::AsyncReadBytesExt;
    ///
    /// #[async_std::main]
    /// async fn main() {
    ///     let mut rdr = Cursor::new(vec![2, 5]);
    ///     assert_eq!(2, rdr.read_u8().await.unwrap());
    ///     assert_eq!(5, rdr.read_u8().await.unwrap());
    /// }
    /// ```
    #[inline]
    fn read_u8<'a>(&'a mut self) -> ReadU8<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadU8(self)
    }

    /// Reads a signed 8 bit integer from the underlying reader.
    ///
    /// Note that since this reads a single byte, no byte order conversions
    /// are used. It is included for completeness.
    ///
    /// # Errors
    ///
    /// This method returns the same errors as [`Read::read_exact`].
    ///
    /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
    ///
    /// # Examples
    ///
    /// Read signed 8 bit integers from a `Read`:
    ///
    /// ```rust
    /// use async_std::io::Cursor;
    /// use async_byteorder::AsyncReadBytesExt;
    ///
    /// #[async_std::main]
    /// async fn main() {
    ///     let mut rdr = Cursor::new(vec![0x02, 0xfb]);
    ///     assert_eq!(2, rdr.read_i8().await.unwrap());
    ///     assert_eq!(-5, rdr.read_i8().await.unwrap());
    /// }
    /// ```
    #[inline]
    fn read_i8<'a>(&'a mut self) -> ReadI8<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadI8(self)
    }

    read_impl! {
        /// Reads an unsigned 16 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read unsigned 16 bit big-endian integers from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![2, 5, 3, 0]);
        ///     assert_eq!(517, rdr.read_u16::<BigEndian>().await.unwrap());
        ///     assert_eq!(768, rdr.read_u16::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_u16(&mut self) -> ReadU16
    }

    read_impl! {
        /// Reads a signed 16 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read signed 16 bit big-endian integers from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![0x00, 0xc1, 0xff, 0x7c]);
        ///     assert_eq!(193, rdr.read_i16::<BigEndian>().await.unwrap());
        ///     assert_eq!(-132, rdr.read_i16::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_i16(&mut self) -> ReadI16
    }

    read_impl! {
        /// Reads an unsigned 24 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read unsigned 24 bit big-endian integers from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![0x00, 0x01, 0x0b]);
        ///     assert_eq!(267, rdr.read_u24::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_u24(&mut self) -> ReadU24
    }

    read_impl! {
        /// Reads a signed 24 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read signed 24 bit big-endian integers from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![0xff, 0x7a, 0x33]);
        ///     assert_eq!(-34253, rdr.read_i24::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_i24(&mut self) -> ReadI24
    }

    read_impl! {
        /// Reads an unsigned 32 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read unsigned 32 bit big-endian integers from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![0x00, 0x00, 0x01, 0x0b]);
        ///     assert_eq!(267, rdr.read_u32::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_u32(&mut self) -> ReadU32
    }

    read_impl! {
        /// Reads a signed 32 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read signed 32 bit big-endian integers from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![0xff, 0xff, 0x7a, 0x33]);
        ///     assert_eq!(-34253, rdr.read_i32::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_i32(&mut self) -> ReadI32
    }

    read_impl! {
        /// Reads an unsigned 48 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read unsigned 48 bit big-endian integers from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![0xb6, 0x71, 0x6b, 0xdc, 0x2b, 0x31]);
        ///     assert_eq!(200598257150769, rdr.read_u48::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_u48(&mut self) -> ReadU48
    }

    read_impl! {
        /// Reads a signed 48 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read signed 48 bit big-endian integers from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![0x9d, 0x71, 0xab, 0xe7, 0x97, 0x8f]);
        ///     assert_eq!(-108363435763825, rdr.read_i48::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_i48(&mut self) -> ReadI48
    }

    read_impl! {
        /// Reads an unsigned 64 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read an unsigned 64 bit big-endian integer from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![0x00, 0x03, 0x43, 0x95, 0x4d, 0x60, 0x86, 0x83]);
        ///     assert_eq!(918733457491587, rdr.read_u64::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_u64(&mut self) -> ReadU64
    }

    read_impl! {
        /// Reads a signed 64 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read a signed 64 bit big-endian integer from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![0x80, 0, 0, 0, 0, 0, 0, 0]);
        ///     assert_eq!(i64::min_value(), rdr.read_i64::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_i64(&mut self) -> ReadI64
    }

    read_impl! {
        /// Reads an unsigned 128 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read an unsigned 128 bit big-endian integer from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![
        ///         0x00, 0x03, 0x43, 0x95, 0x4d, 0x60, 0x86, 0x83,
        ///         0x00, 0x03, 0x43, 0x95, 0x4d, 0x60, 0x86, 0x83
        ///     ]);
        ///     assert_eq!(16947640962301618749969007319746179, rdr.read_u128::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_u128(&mut self) -> ReadU128
    }

    read_impl! {
        /// Reads a signed 128 bit integer from the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read a signed 128 bit big-endian integer from a `Read`:
        ///
        /// ```rust
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        ///     assert_eq!(i128::min_value(), rdr.read_i128::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_i128(&mut self) -> ReadI128
    }

    // TODO: read_*int

    read_impl! {
        /// Reads a IEEE754 single-precision (4 bytes) floating point number from
        /// the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read a big-endian single-precision floating point number from a `Read`:
        ///
        /// ```rust
        /// use std::f32;
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![
        ///         0x40, 0x49, 0x0f, 0xdb,
        ///     ]);
        ///     assert_eq!(f32::consts::PI, rdr.read_f32::<BigEndian>().await.unwrap());
        /// }
        /// ```
        fn read_f32(&mut self) -> ReadF32
    }

    read_impl! {
        /// Reads a IEEE754 double-precision (8 bytes) floating point number from
        /// the underlying reader.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Read::read_exact`].
        ///
        /// [`Read::read_exact`]: https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact
        ///
        /// # Examples
        ///
        /// Read a big-endian double-precision floating point number from a `Read`:
        ///
        /// ```rust
        /// use std::f64;
        /// use async_std::io::Cursor;
        /// use async_byteorder::{BigEndian, AsyncReadBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut rdr = Cursor::new(vec![
        ///         0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18,
        ///     ]);
        ///     assert_eq!(f64::consts::PI, rdr.read_f64::<BigEndian>().await.unwrap());
        /// }
        /// ```
    fn read_f64(&mut self) -> ReadF64
    }

    // TODO: read_*_into
}

/// All types that implement `AsyncRead` get methods defined in `AsyncReadBytesExt`
/// for free.
impl<R: io::AsyncRead + ?Sized> AsyncReadBytesExt for R {}

macro_rules! writer {
    ($name:ident, $ty:ty, $writer:ident) => {
        writer!($name, $ty, $writer, size_of::<$ty>());
    };
    ($name:ident, $ty:ty, $writer:ident, $bytes:expr) => {
        #[doc(hidden)]
        pub struct $name<W> {
            buf: [u8; $bytes],
            written: u8,
            dst: W,
        }

        impl<W> $name<W> {
            fn new<T: ByteOrder>(w: W, value: $ty) -> Self {
                let mut writer = $name {
                    buf: [0; $bytes],
                    written: 0,
                    dst: w,
                };
                T::$writer(&mut writer.buf[..], value);
                writer
            }
        }

        impl<W> Future for $name<W>
        where
            W: io::AsyncWrite,
        {
            type Output = io::Result<()>;
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                if self.written == $bytes as u8 {
                    return Poll::Ready(Ok(()));
                }

                // we need this so that we can mutably borrow multiple fields
                // it is safe as long as we never take &mut to dst (since it has been pinned)
                // unless it is to place it in a Pin itself like below.
                let mut this = unsafe { self.get_unchecked_mut() };
                let mut dst = unsafe { Pin::new_unchecked(&mut this.dst) };

                while this.written < $bytes as u8 {
                    this.written += match dst
                        .as_mut()
                        .poll_write(cx, &this.buf[this.written as usize..])
                    {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
                        Poll::Ready(Ok(n)) => n as u8,
                    };
                }
                Poll::Ready(Ok(()))
            }
        }
    };
}

macro_rules! writer8 {
    ($name:ident, $ty:ty) => {
        #[doc(hidden)]
        pub struct $name<W>(W, $ty);
        impl<W> Future for $name<W>
        where
            W: io::AsyncWrite,
        {
            type Output = io::Result<()>;
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = unsafe { self.get_unchecked_mut() };
                let dst = unsafe { Pin::new_unchecked(&mut this.0) };
                let buf = [this.1 as u8];
                match dst.poll_write(cx, &buf[..]) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
                    Poll::Ready(Ok(0)) => Poll::Pending,
                    Poll::Ready(Ok(1)) => Poll::Ready(Ok(())),
                    Poll::Ready(Ok(_)) => unreachable!(),
                }
            }
        }
    };
}

writer8!(WriteU8, u8);
writer8!(WriteI8, i8);

writer!(WriteF32, f32, write_f32);
writer!(WriteF64, f64, write_f64);
writer!(WriteU16, u16, write_u16);
writer!(WriteU24, u32, write_u24, 3);
writer!(WriteU32, u32, write_u32);
writer!(WriteU48, u64, write_u48, 6);
writer!(WriteU64, u64, write_u64);
writer!(WriteU128, u128, write_u128);
writer!(WriteI16, i16, write_i16);
writer!(WriteI24, i32, write_i24, 3);
writer!(WriteI32, i32, write_i32);
writer!(WriteI48, i64, write_i48, 6);
writer!(WriteI64, i64, write_i64);
writer!(WriteI128, i128, write_i128);

macro_rules! write_impl {
    (
        $(#[$outer:meta])*
        fn $name:ident(&mut self, n: $ty:ty) -> $($fut:tt)*
    ) => {
        $(#[$outer])*
        #[inline]
        fn $name<'a, T: ByteOrder>(&'a mut self, n: $ty) -> $($fut)*<&'a mut Self> where Self: Unpin {
            $($fut)*::new::<T>(self, n)
        }
    }
}

/// Extends [`AsyncWrite`] with methods for writing numbers.
///
/// Most of the methods defined here have an unconstrained type parameter that
/// must be explicitly instantiated. Typically, it is instantiated with either
/// the [`BigEndian`] or [`LittleEndian`] types defined in this crate.
///
/// # Examples
///
/// Write unsigned 16 bit big-endian integers to a [`Write`]:
///
/// ```rust
/// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
///
/// #[async_std::main]
/// async fn main() {
///     let mut wtr = vec![];
///     wtr.write_u16::<BigEndian>(517).await.unwrap();
///     wtr.write_u16::<BigEndian>(768).await.unwrap();
///     assert_eq!(wtr, vec![2, 5, 3, 0]);
/// }
/// ```
///
/// [`BigEndian`]: enum.BigEndian.html
/// [`LittleEndian`]: enum.LittleEndian.html
/// [`AsyncWrite`]: https://docs.rs/tokio/0.2.0-alpha.4/tokio/io/trait.AsyncWrite.html
pub trait AsyncWriteBytesExt: io::AsyncWrite {
    /// Writes an unsigned 8 bit integer to the underlying writer.
    ///
    /// Note that since this writes a single byte, no byte order conversions
    /// are used. It is included for completeness.
    ///
    /// # Errors
    ///
    /// This method returns the same errors as [`Write::write_all`].
    ///
    /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
    ///
    /// # Examples
    ///
    /// Write unsigned 8 bit integers to a `Write`:
    ///
    /// ```rust
    /// use async_byteorder::{AsyncWriteBytesExt};
    ///
    /// #[async_std::main]
    /// async fn main() {
    ///     let mut wtr = Vec::new();
    ///     wtr.write_u8(2).await.unwrap();
    ///     wtr.write_u8(5).await.unwrap();
    ///     assert_eq!(wtr, b"\x02\x05");
    /// }
    /// ```
    #[inline]
    fn write_u8<'a>(&'a mut self, n: u8) -> WriteU8<&'a mut Self>
    where
        Self: Unpin,
    {
        WriteU8(self, n)
    }

    /// Writes a signed 8 bit integer to the underlying writer.
    ///
    /// Note that since this writes a single byte, no byte order conversions
    /// are used. It is included for completeness.
    ///
    /// # Errors
    ///
    /// This method returns the same errors as [`Write::write_all`].
    ///
    /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
    ///
    /// # Examples
    ///
    /// Write signed 8 bit integers to a `Write`:
    ///
    /// ```rust
    /// use async_byteorder::{AsyncWriteBytesExt};
    ///
    /// #[async_std::main]
    /// async fn main() {
    ///     let mut wtr = Vec::new();
    ///     wtr.write_i8(2).await.unwrap();
    ///     wtr.write_i8(-5).await.unwrap();
    ///     assert_eq!(wtr, b"\x02\xfb");
    /// }
    /// ```
    #[inline]
    fn write_i8<'a>(&'a mut self, n: i8) -> WriteI8<&'a mut Self>
    where
        Self: Unpin,
    {
        WriteI8(self, n)
    }

    write_impl! {
        /// Writes an unsigned 16 bit integer to the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write unsigned 16 bit big-endian integers to a `Write`:
        ///
        /// ```rust
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_u16::<BigEndian>(517).await.unwrap();
        ///     wtr.write_u16::<BigEndian>(768).await.unwrap();
        ///     assert_eq!(wtr, b"\x02\x05\x03\x00");
        /// }
        /// ```
        fn write_u16(&mut self, n: u16) -> WriteU16
    }

    write_impl! {
        /// Writes a signed 16 bit integer to the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write signed 16 bit big-endian integers to a `Write`:
        ///
        /// ```rust
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_i16::<BigEndian>(193).await.unwrap();
        ///     wtr.write_i16::<BigEndian>(-132).await.unwrap();
        ///     assert_eq!(wtr, b"\x00\xc1\xff\x7c");
        /// }
        /// ```
        fn write_i16(&mut self, n: i16) -> WriteI16
    }

    write_impl! {
        /// Writes an unsigned 24 bit integer to the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write unsigned 24 bit big-endian integers to a `Write`:
        ///
        /// ```rust
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_u24::<BigEndian>(267).await.unwrap();
        ///     wtr.write_u24::<BigEndian>(120111).await.unwrap();
        ///     assert_eq!(wtr, b"\x00\x01\x0b\x01\xd5\x2f");
        /// }
        /// ```
        fn write_u24(&mut self, n: u32) -> WriteU24
    }

    write_impl! {
        /// Writes a signed 24 bit integer to the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write signed 24 bit big-endian integers to a `Write`:
        ///
        /// ```rust
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_i24::<BigEndian>(-34253).await.unwrap();
        ///     wtr.write_i24::<BigEndian>(120111).await.unwrap();
        ///     assert_eq!(wtr, b"\xff\x7a\x33\x01\xd5\x2f");
        /// }
        /// ```
        fn write_i24(&mut self, n: i32) -> WriteI24
    }

    write_impl! {
        /// Writes an unsigned 32 bit integer to the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write unsigned 32 bit big-endian integers to a `Write`:
        ///
        /// ```rust
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_u32::<BigEndian>(267).await.unwrap();
        ///     wtr.write_u32::<BigEndian>(1205419366).await.unwrap();
        ///     assert_eq!(wtr, b"\x00\x00\x01\x0b\x47\xd9\x3d\x66");
        /// }
        /// ```
        fn write_u32(&mut self, n: u32) -> WriteU32
    }

    write_impl! {
        /// Writes a signed 32 bit integer to the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write signed 32 bit big-endian integers to a `Write`:
        ///
        /// ```rust
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_i32::<BigEndian>(-34253).await.unwrap();
        ///     wtr.write_i32::<BigEndian>(1205419366).await.unwrap();
        ///     assert_eq!(wtr, b"\xff\xff\x7a\x33\x47\xd9\x3d\x66");
        /// }
        /// ```
        fn write_i32(&mut self, n: i32) -> WriteI32
    }

    write_impl! {
        /// Writes an unsigned 48 bit integer to the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write unsigned 48 bit big-endian integers to a `Write`:
        ///
        /// ```rust
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_u48::<BigEndian>(52360336390828).await.unwrap();
        ///     wtr.write_u48::<BigEndian>(541).await.unwrap();
        ///     assert_eq!(wtr, b"\x2f\x9f\x17\x40\x3a\xac\x00\x00\x00\x00\x02\x1d");
        /// }
        /// ```
        fn write_u48(&mut self, n: u64) -> WriteU48
    }

    write_impl! {
        /// Writes a signed 48 bit integer to the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write signed 48 bit big-endian integers to a `Write`:
        ///
        /// ```rust
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_i48::<BigEndian>(-108363435763825).await.unwrap();
        ///     wtr.write_i48::<BigEndian>(77).await.unwrap();
        ///     assert_eq!(wtr, b"\x9d\x71\xab\xe7\x97\x8f\x00\x00\x00\x00\x00\x4d");
        /// }
        /// ```
        fn write_i48(&mut self, n: i64) -> WriteI48
    }

    write_impl! {
        /// Writes an unsigned 64 bit integer to the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write unsigned 64 bit big-endian integers to a `Write`:
        ///
        /// ```rust
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_u64::<BigEndian>(918733457491587).await.unwrap();
        ///     wtr.write_u64::<BigEndian>(143).await.unwrap();
        ///     assert_eq!(wtr, b"\x00\x03\x43\x95\x4d\x60\x86\x83\x00\x00\x00\x00\x00\x00\x00\x8f");
        /// }
        /// ```
        fn write_u64(&mut self, n: u64) -> WriteU64
    }

    write_impl! {
        /// Writes a signed 64 bit integer to the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write signed 64 bit big-endian integers to a `Write`:
        ///
        /// ```rust
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_i64::<BigEndian>(i64::min_value()).await.unwrap();
        ///     wtr.write_i64::<BigEndian>(i64::max_value()).await.unwrap();
        ///     assert_eq!(wtr, b"\x80\x00\x00\x00\x00\x00\x00\x00\x7f\xff\xff\xff\xff\xff\xff\xff");
        /// }
        /// ```
        fn write_i64(&mut self, n: i64) -> WriteI64
    }

    write_impl! {
        /// Writes an unsigned 128 bit integer to the underlying writer.
        fn write_u128(&mut self, n: u128) -> WriteU128
    }

    write_impl! {
        /// Writes a signed 128 bit integer to the underlying writer.
        fn write_i128(&mut self, n: i128) -> WriteI128
    }

    // TODO: write_*int

    write_impl! {
        /// Writes a IEEE754 single-precision (4 bytes) floating point number to
        /// the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write a big-endian single-precision floating point number to a `Write`:
        ///
        /// ```rust
        /// use std::f32;
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_f32::<BigEndian>(f32::consts::PI).await.unwrap();
        ///     assert_eq!(wtr, b"\x40\x49\x0f\xdb");
        /// }
        /// ```
        fn write_f32(&mut self, n: f32) -> WriteF32
    }

    write_impl! {
        /// Writes a IEEE754 double-precision (8 bytes) floating point number to
        /// the underlying writer.
        ///
        /// # Errors
        ///
        /// This method returns the same errors as [`Write::write_all`].
        ///
        /// [`Write::write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        ///
        /// # Examples
        ///
        /// Write a big-endian double-precision floating point number to a `Write`:
        ///
        /// ```rust
        /// use std::f64;
        /// use async_byteorder::{BigEndian, AsyncWriteBytesExt};
        ///
        /// #[async_std::main]
        /// async fn main() {
        ///     let mut wtr = Vec::new();
        ///     wtr.write_f64::<BigEndian>(f64::consts::PI).await.unwrap();
        ///     assert_eq!(wtr, b"\x40\x09\x21\xfb\x54\x44\x2d\x18");
        /// }
        /// ```
        fn write_f64(&mut self, n: f64) -> WriteF64
    }
}

/// All types that implement `Write` get methods defined in `WriteBytesExt`
/// for free.
impl<W: io::AsyncWrite + ?Sized> AsyncWriteBytesExt for W {}
