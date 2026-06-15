use std::num::NonZeroUsize;

use crate::{
    ChannelConfig, QueueConfig, VectorConfig,
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
    fn from_config(config: &ChannelConfig) -> Self {
        Self {
            additional_messages: config.queue.additional_messages as u32,
            message_size: config.queue.message_size.get() as u32,
            eventfd: config.eventfd as u32,
            info_size: config.info.len() as u32,
        }
    }
}

struct Layout {
    vector_info_offset: usize,
    num_channels: [usize; 2],
    channel_table: usize,
    vector_info: usize,
    channel_infos: usize,
    size: usize,
}

impl Layout {
    pub(self) fn calc(vconfig: &VectorConfig) -> Self {
        let mut offset = HEADER_SIZE;

        let vector_info_offset = offset;
        offset += size_of::<u32>();

        let num_channels: [usize; 2] = [offset, offset + size_of::<u32>()];
        offset += 2 * size_of::<u32>();

        let channel_table: usize = offset;

        offset += (vconfig.producers.len() + vconfig.consumers.len()) * size_of::<ChannelEntry>();

        let vector_info = offset;
        offset += vconfig.info.len();

        let channel_infos = offset;

        for config in &vconfig.producers {
            offset += config.info.len();
        }

        for config in &vconfig.consumers {
            offset += config.info.len();
        }

        let size = offset;

        Self {
            vector_info_offset,
            num_channels,
            channel_table,
            vector_info,
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
    config: &ChannelConfig,
    entry_offset: &mut usize,
    info_offset: &mut usize,
) {
    let entry_ptr = req_get_mut_ptr::<ChannelEntry>(request, *entry_offset).unwrap();
    unsafe {
        entry_ptr.write_unaligned(ChannelEntry::from_config(config));
    }

    if !config.info.is_empty() {
        request[*info_offset..*info_offset + config.info.len()]
            .clone_from_slice(config.info.as_slice());
        *info_offset += config.info.len();
    }
    *entry_offset += size_of::<ChannelEntry>();
}

fn request_read_entry(
    request: &[u8],
    entry_offset: &mut usize,
    info_offset: &mut usize,
) -> Result<ChannelConfig, RequestError> {
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

    Ok(ChannelConfig {
        queue: QueueConfig {
            additional_messages: entry.additional_messages as usize,
            message_size,
        },
        eventfd: entry.eventfd != 0,
        info,
    })
}

pub fn parse_request(request: &[u8]) -> Result<VectorConfig, RequestError> {
    let header = request
        .get(0..HEADER_SIZE)
        .ok_or(RequestError::OutOfBounds)?;

    verify_header(header).inspect_err(|e| {
        error!("parse header failed {e:?}");
    })?;

    let mut offset: usize = HEADER_SIZE;

    let vector_info_size = request_read::<u32>(request, offset).inspect_err(|_| {
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

    let vector_info_offset = offset + (num_consumers + num_producers) * size_of::<ChannelEntry>();

    let mut channel_info_offset = vector_info_offset + vector_info_size;

    if channel_info_offset > request.len() {
        error!("request message too small for vector info");
        return Err(RequestError::OutOfBounds);
    }

    let info: Vec<u8> = request[vector_info_offset..channel_info_offset].to_vec();

    let mut consumers: Vec<ChannelConfig> = Vec::with_capacity(num_consumers);
    let mut producers: Vec<ChannelConfig> = Vec::with_capacity(num_producers);

    for _ in 0..num_consumers {
        let config = request_read_entry(request, &mut offset, &mut channel_info_offset)?;

        consumers.push(config);
    }

    for _ in 0..num_producers {
        let config = request_read_entry(request, &mut offset, &mut channel_info_offset)?;

        producers.push(config);
    }

    Ok(VectorConfig {
        consumers,
        producers,
        info,
    })
}

pub fn create_request(vconfig: &VectorConfig) -> Vec<u8> {
    let layout = Layout::calc(vconfig);

    let mut request: Vec<u8> = vec![0; layout.size];

    write_header(request.as_mut_slice());

    request_write(
        request.as_mut_slice(),
        layout.vector_info_offset,
        &(vconfig.info.len() as u32),
    )
    .unwrap();

    request_write(
        request.as_mut_slice(),
        layout.num_channels[0],
        &(vconfig.producers.len() as u32),
    )
    .unwrap();

    request_write(
        request.as_mut_slice(),
        layout.num_channels[1],
        &(vconfig.consumers.len() as u32),
    )
    .unwrap();

    let mut entry_offset = layout.channel_table;

    request[layout.vector_info..layout.vector_info + vconfig.info.len()]
        .clone_from_slice(vconfig.info.as_slice());

    let mut info_offset = layout.channel_infos;

    vconfig
        .producers
        .iter()
        .for_each(|c| request_write_channel(&mut request, c, &mut entry_offset, &mut info_offset));

    vconfig
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
