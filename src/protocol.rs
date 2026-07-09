use std::num::NonZeroUsize;

use crate::{
    ChannelAttr, GroupAttr, QueueAttr,
    error::*,
    header::{HEADER_SIZE, verify_header, write_header},
    log::error,
};

#[repr(C)]
struct ChannelEntry {
    additional_messages: u32,
    message_size: u32,
    eventfd: u32,
    info_size: u32,
}

impl ChannelEntry {
    fn from_attr(attr: &ChannelAttr) -> Self {
        Self {
            additional_messages: attr.additional_messages as u32,
            message_size: attr.message_size.get() as u32,
            eventfd: attr.eventfd as u32,
            info_size: attr.info.len() as u32,
        }
    }
}

struct Layout {
    group_info_offset: usize,
    num_channels: [usize; 2],
    channel_table: usize,
    group_info: usize,
    channel_infos: usize,
    size: usize,
}

impl Layout {
    pub(self) fn calc(config: &GroupAttr) -> Self {
        let mut offset = HEADER_SIZE;

        let group_info_offset = offset;
        offset += size_of::<u32>();

        let num_channels: [usize; 2] = [offset, offset + size_of::<u32>()];
        offset += 2 * size_of::<u32>();

        let channel_table: usize = offset;

        offset += (config.producers.len() + config.consumers.len()) * size_of::<ChannelEntry>();

        let group_info = offset;
        offset += config.info.len();

        let channel_infos = offset;

        for attr in &config.producers {
            offset += attr.info.len();
        }

        for attr in &config.consumers {
            offset += attr.info.len();
        }

        let size = offset;

        Self {
            group_info_offset,
            num_channels,
            channel_table,
            group_info,
            channel_infos,
            size,
        }
    }
}

fn request_read<T>(request: &[u8], offset: usize) -> Result<T, RequestError> {
    if offset + size_of::<T>() > request.len() {
        return Err(RequestError::OutOfBounds);
    }

    let ptr = unsafe { request.as_ptr().byte_add(offset) as *const T };

    Ok(unsafe { ptr.read_unaligned() })
}

fn req_get_mut_ptr<T>(request: &mut [u8], offset: usize) -> Result<*mut T, RequestError> {
    if offset + size_of::<T>() > request.len() {
        return Err(RequestError::OutOfBounds);
    }

    let ptr = unsafe { request.as_mut_ptr().byte_add(offset) as *mut T };

    Ok(ptr)
}

fn request_write<T: Copy>(request: &[u8], offset: usize, val: &T) -> Result<(), RequestError> {
    if offset + size_of::<T>() > request.len() {
        return Err(RequestError::OutOfBounds);
    }

    let ptr = unsafe { request.as_ptr().byte_add(offset) as *mut T };

    unsafe {
        ptr.write_unaligned(*val);
    }

    Ok(())
}

fn request_write_channel(
    request: &mut [u8],
    attr: &ChannelAttr,
    entry_offset: &mut usize,
    info_offset: &mut usize,
) {
    let entry_ptr = req_get_mut_ptr::<ChannelEntry>(request, *entry_offset).unwrap();
    unsafe {
        entry_ptr.write_unaligned(ChannelEntry::from_attr(attr));
    }

    if !attr.info.is_empty() {
        request[*info_offset..*info_offset + attr.info.len()]
            .clone_from_slice(attr.info.as_slice());
        *info_offset += attr.info.len();
    }
    *entry_offset += size_of::<ChannelEntry>();
}

fn request_read_entry(
    request: &[u8],
    entry_offset: &mut usize,
    info_offset: &mut usize,
) -> Result<ChannelAttr, RequestError> {
    let entry = request_read::<ChannelEntry>(request, *entry_offset).inspect_err(|_| {
        error!("request message too short");
    })?;

    if entry.message_size == 0 {
        error!("request: message size = 0 not allowed");
        return Err(RequestError::OutOfBounds);
    }

    let message_size = NonZeroUsize::new(entry.message_size as usize).unwrap();

    let info_size = entry.info_size as usize;

    if *info_offset + info_size > request.len() {
        error!("request message too small for channel infos");
        return Err(RequestError::OutOfBounds);
    }

    let info = match info_size {
        0 => Vec::with_capacity(0),
        _ => request[*info_offset..*info_offset + info_size].to_vec(),
    };

    *entry_offset += size_of::<ChannelEntry>();
    *info_offset += info_size;

    Ok(ChannelAttr {
        additional_messages: entry.additional_messages as usize,
        message_size,
        eventfd: entry.eventfd != 0,
        info,
    })
}

pub fn parse_request(request: &[u8]) -> Result<GroupAttr, RequestError> {
    let header = request
        .get(0..HEADER_SIZE)
        .ok_or(RequestError::OutOfBounds)?;

    verify_header(header).inspect_err(|e| {
        error!("parse header failed {e:?}");
    })?;

    let mut offset: usize = HEADER_SIZE;

    let group_info_size = request_read::<u32>(request, offset).inspect_err(|_| {
        error!("request message too short");
    })? as usize;
    offset += size_of::<u32>();

    let num_consumers = request_read::<u32>(request, offset).inspect_err(|_| {
        error!("request message too small");
    })? as usize;
    offset += size_of::<u32>();

    let num_producers = request_read::<u32>(request, offset).inspect_err(|_| {
        error!("request message too small");
    })? as usize;
    offset += size_of::<u32>();

    let group_info_offset = offset + (num_consumers + num_producers) * size_of::<ChannelEntry>();

    let mut channel_info_offset = group_info_offset + group_info_size;

    if channel_info_offset > request.len() {
        error!("request message too small for vector info");
        return Err(RequestError::OutOfBounds);
    }

    let info: Vec<u8> = request[group_info_offset..channel_info_offset].to_vec();

    let mut consumers: Vec<ChannelAttr> = Vec::with_capacity(num_consumers);
    let mut producers: Vec<ChannelAttr> = Vec::with_capacity(num_producers);

    for _ in 0..num_consumers {
        let attr = request_read_entry(request, &mut offset, &mut channel_info_offset)?;

        consumers.push(attr);
    }

    for _ in 0..num_producers {
        let attr = request_read_entry(request, &mut offset, &mut channel_info_offset)?;

        producers.push(attr);
    }

    Ok(GroupAttr {
        consumers,
        producers,
        info,
    })
}

pub fn create_request(config: &GroupAttr) -> Vec<u8> {
    let layout = Layout::calc(config);

    let mut request: Vec<u8> = vec![0; layout.size];

    write_header(request.as_mut_slice());

    request_write(
        request.as_mut_slice(),
        layout.group_info_offset,
        &(config.info.len() as u32),
    )
    .unwrap();

    request_write(
        request.as_mut_slice(),
        layout.num_channels[0],
        &(config.producers.len() as u32),
    )
    .unwrap();

    request_write(
        request.as_mut_slice(),
        layout.num_channels[1],
        &(config.consumers.len() as u32),
    )
    .unwrap();

    let mut entry_offset = layout.channel_table;

    request[layout.group_info..layout.group_info + config.info.len()]
        .clone_from_slice(config.info.as_slice());

    let mut info_offset = layout.channel_infos;

    config
        .producers
        .iter()
        .for_each(|c| request_write_channel(&mut request, c, &mut entry_offset, &mut info_offset));

    config
        .consumers
        .iter()
        .for_each(|c| request_write_channel(&mut request, c, &mut entry_offset, &mut info_offset));

    request
}

pub(crate) fn create_response(success: bool) -> Vec<u8> {
    if success {
        vec![0, 0, 0, 0]
    } else {
        vec![0xff, 0xff, 0xff, 0xff]
    }
}

pub(crate) fn parse_response(response: &[u8]) -> Result<(), TransferError> {
    if response != vec![0, 0, 0, 0] {
        Err(TransferError::ResponseError)
    } else {
        Ok(())
    }
}
