use super::*;
use bytes::{Buf, Bytes};

pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Unsubscribe, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);

    let pkid = read_u16(&mut bytes)?;
    let mut payload_bytes = fixed_header.remaining_len - 2;
    let mut filters = Vec::with_capacity(1);

    while payload_bytes > 0 {
        let topic_filter = read_mqtt_string(&mut bytes)?;
        payload_bytes -= topic_filter.len() + 2;
        filters.push(topic_filter);
    }

    let unsubscribe = Unsubscribe { pkid, filters };
    Ok(unsubscribe)
}

pub fn write(unsubscribe: &Unsubscribe, buffer: &mut BytesMut) -> Result<usize, Error> {
    let remaining_len = 2 + unsubscribe
        .filters
        .iter()
        .fold(0, |s, topic| s + topic.len() + 2);

    buffer.put_u8(0xA2);
    let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;
    buffer.put_u16(unsubscribe.pkid);

    for topic in unsubscribe.filters.iter() {
        write_mqtt_string(buffer, topic.as_str());
    }
    Ok(1 + remaining_len_bytes + remaining_len)
}
