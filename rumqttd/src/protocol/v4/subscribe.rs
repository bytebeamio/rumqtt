use super::*;
use bytes::{Buf, Bytes};

fn len(subscribe: &Subscribe) -> usize {
    // len of pkid + vec![subscribe filter len]
    2 + subscribe.filters.iter().fold(0, |s, t| s + filter::len(t))
}

pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Subscribe, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);

    let pkid = read_u16(&mut bytes)?;
    let filters = filter::read(&mut bytes)?;

    match filters.len() {
        0 => Err(Error::EmptySubscription),
        _ => Ok(Subscribe { pkid, filters }),
    }
}

pub fn write(subscribe: &Subscribe, buffer: &mut BytesMut) -> Result<usize, Error> {
    // write packet type
    buffer.put_u8(0x82);

    // write remaining length
    let remaining_len = len(subscribe);
    let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

    // write packet id
    buffer.put_u16(subscribe.pkid);

    // write filters
    for f in subscribe.filters.iter() {
        filter::write(f, buffer);
    }

    Ok(1 + remaining_len_bytes + remaining_len)
}

mod filter {
    use super::*;

    pub fn len(filter: &Filter) -> usize {
        // filter len + filter + options
        2 + filter.path.len() + 1
    }

    pub fn read(bytes: &mut Bytes) -> Result<Vec<Filter>, Error> {
        // variable header size = 2 (packet identifier)
        let mut filters = Vec::new();

        while bytes.has_remaining() {
            let path = read_mqtt_bytes(bytes)?;
            let path = std::str::from_utf8(&path)?.to_owned();
            let options = read_u8(bytes)?;
            let requested_qos = options & 0b0000_0011;

            filters.push(Filter {
                path,
                qos: qos(requested_qos).ok_or(Error::InvalidQoS(requested_qos))?,
                nolocal: false,
                preserve_retain: false,
                retain_forward_rule: RetainForwardRule::OnEverySubscribe,
            });
        }

        Ok(filters)
    }

    pub fn write(filter: &Filter, buffer: &mut BytesMut) {
        let mut options = 0;
        options |= filter.qos as u8;

        write_mqtt_string(buffer, filter.path.as_str());
        buffer.put_u8(options);
    }
}
