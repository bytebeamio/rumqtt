use crate::protocol::Publish;
use crate::Storage;
use bytes::Bytes;

impl Storage for Bytes {
    fn size(&self) -> usize {
        // For bytes len returns number of bytes in the given `Bytes`
        self.len()
    }
}

impl Storage for Publish {
    fn size(&self) -> usize {
        4 + self.topic.len() + self.payload.len()
    }
}

impl Storage for Vec<u8> {
    fn size(&self) -> usize {
        // For bytes len returns number of bytes in the given `Bytes`
        self.len()
    }
}
