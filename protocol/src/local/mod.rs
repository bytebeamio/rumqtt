use crate::Protocol;

#[derive(Debug, Clone, Copy)]
pub struct Local;

impl Protocol for Local {
    fn check(&self, stream: &[u8], max_size: usize) -> Result<crate::Frame, crate::Error> {
        todo!()
    }

    fn frame(
        &self,
        stream: &mut &[u8],
        frames: &mut Vec<u8>,
        max_size: usize,
    ) -> Result<crate::Frame, crate::Error> {
        todo!()
    }

    fn read_connect(
        &self,
        stream: &mut &[u8],
        max_size: usize,
    ) -> Result<crate::Connect, crate::Error> {
        todo!()
    }

    fn read_publish(
        &self,
        stream: &mut &[u8],
        max_size: usize,
    ) -> Result<crate::Publish, crate::Error> {
        todo!()
    }

    fn write_connect(
        &self,
        connect: &crate::Connect,
        buffer: &mut Vec<u8>,
    ) -> Result<usize, crate::Error> {
        todo!()
    }

    fn write_publish(
        &self,
        publish: &crate::Publish,
        buffer: &mut Vec<u8>,
    ) -> Result<usize, crate::Error> {
        todo!()
    }

    fn read(&self, stream: &mut &[u8], max_size: usize) -> Result<crate::Packet, crate::Error> {
        todo!()
    }

    fn write(&self, packet: crate::Packet, write: &mut Vec<u8>) -> Result<usize, crate::Error> {
        todo!()
    }
}
