macro_rules! rtt {
    ($name:ident, $write:path, $read:path, $v:expr) => {
        #[async_std::test]
        async fn $name() -> futures_io::Result<()> {
            #[allow(unused_imports)]
            use async_byteorder::{AsyncReadBytesExt, AsyncWriteBytesExt, BigEndian};

            let mut bytes = Vec::new();
            let () = $write(&mut bytes, $v).await?;
            let mut b = &bytes[..];
            let v = $read(&mut b).await?;
            assert_eq!(v, $v);
            Ok(())
        }
    };
}

macro_rules! writes {
    ($write:path, $v:expr) => {
        #[async_std::test]
        async fn writes() -> futures_io::Result<()> {
            #[allow(unused_imports)]
            use async_byteorder::{AsyncWriteBytesExt, BigEndian};

            let mut bytes = Vec::new();
            let () = $write(&mut bytes, $v).await?;
            assert_ne!(bytes.len(), 0);
            Ok(())
        }
    };
}

macro_rules! rtts {
    ($name:ident, $write:path, $read:path, $ty:tt) => {
        mod $name {
            rtt!(zero, $write, $read, 0 as $ty);
            rtt!(min, $write, $read, $ty::min_value());
            rtt!(max, $write, $read, $ty::max_value());
            writes!($write, 0 as $ty);
        }
    };
}

rtts!(
    u8,
    AsyncWriteBytesExt::write_u8,
    AsyncReadBytesExt::read_u8,
    u8
);
rtts!(
    i8,
    AsyncWriteBytesExt::write_i8,
    AsyncReadBytesExt::read_i8,
    i8
);
rtts!(
    u16,
    AsyncWriteBytesExt::write_u16::<BigEndian>,
    AsyncReadBytesExt::read_u16::<BigEndian>,
    u16
);
rtts!(
    i16,
    AsyncWriteBytesExt::write_i16::<BigEndian>,
    AsyncReadBytesExt::read_i16::<BigEndian>,
    i16
);
rtts!(
    u32,
    AsyncWriteBytesExt::write_u32::<BigEndian>,
    AsyncReadBytesExt::read_u32::<BigEndian>,
    u32
);
rtts!(
    i32,
    AsyncWriteBytesExt::write_i32::<BigEndian>,
    AsyncReadBytesExt::read_i32::<BigEndian>,
    i32
);
rtts!(
    u64,
    AsyncWriteBytesExt::write_u64::<BigEndian>,
    AsyncReadBytesExt::read_u64::<BigEndian>,
    u64
);
rtts!(
    i64,
    AsyncWriteBytesExt::write_i64::<BigEndian>,
    AsyncReadBytesExt::read_i64::<BigEndian>,
    i64
);
