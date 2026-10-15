use gstreamer::ClockTime;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use gstreamer::{prelude::*, Bin, Caps, Element, ElementFactory, FlowError, GhostPad};
use gstreamer_app::{AppSrc, AppSrcCallbacks, AppStreamType};
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{
        BcMedia, BcMediaAac, BcMediaIframe, BcMediaInfoV1, BcMediaInfoV2, BcMediaPframe, VideoType,
    },
};
use tokio::{sync::mpsc::channel as mpsc, task::JoinHandle, time::timeout};

use crate::{common::NeoInstance, rtsp::gst::NeoMediaFactory, AnyResult};

#[derive(Clone, Debug)]
pub enum AudioType {
    Aac,
    Adpcm(u32),
}

#[derive(Clone, Debug)]
struct StreamConfig {
    #[allow(dead_code)]
    resolution: [u32; 2],
    bitrate: u32,
    fps: u32,
    bitrate_table: Vec<u32>,
    fps_table: Vec<u32>,
    vid_type: Option<VideoType>,
    aud_type: Option<AudioType>,
}
impl StreamConfig {
    async fn new(instance: &NeoInstance, name: StreamKind) -> AnyResult<Self> {
        let (resolution, bitrate, fps, fps_table, bitrate_table) = instance
            .run_passive_task(|cam| {
                Box::pin(async move {
                    let infos = cam
                        .get_stream_info()
                        .await?
                        .stream_infos
                        .iter()
                        .flat_map(|info| info.encode_tables.clone())
                        .collect::<Vec<_>>();
                    if let Some(encode) =
                        infos.iter().find(|encode| encode.name == name.to_string())
                    {
                        let bitrate_table = encode
                            .bitrate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();
                        let framerate_table = encode
                            .framerate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();

                        Ok((
                            [encode.resolution.width, encode.resolution.height],
                            bitrate_table
                                .get(encode.default_bitrate as usize)
                                .copied()
                                .unwrap_or(encode.default_bitrate)
                                * 1024,
                            framerate_table
                                .get(encode.default_framerate as usize)
                                .copied()
                                .unwrap_or(encode.default_framerate),
                            framerate_table.clone(),
                            bitrate_table.clone(),
                        ))
                    } else {
                        Ok(([0, 0], 0, 0, vec![], vec![]))
                    }
                })
            })
            .await?;

        Ok(StreamConfig {
            resolution,
            bitrate,
            fps,
            fps_table,
            bitrate_table,
            vid_type: None,
            aud_type: None,
        })
    }

    fn update_fps(&mut self, fps: u32) {
        let new_fps = self.fps_table.get(fps as usize).copied().unwrap_or(fps);
        self.fps = new_fps;
    }
    #[allow(dead_code)]
    fn update_bitrate(&mut self, bitrate: u32) {
        let new_bitrate = self
            .bitrate_table
            .get(bitrate as usize)
            .copied()
            .unwrap_or(bitrate);
        self.bitrate = new_bitrate;
    }

    fn update_from_media(&mut self, media: &BcMedia) {
        match media {
            BcMedia::InfoV1(BcMediaInfoV1 { fps, .. })
            | BcMedia::InfoV2(BcMediaInfoV2 { fps, .. }) => self.update_fps(*fps as u32),
            BcMedia::Aac(_) => {
                self.aud_type = Some(AudioType::Aac);
            }
            BcMedia::Adpcm(adpcm) => {
                self.aud_type = Some(AudioType::Adpcm(adpcm.block_size()));
            }
            BcMedia::Iframe(BcMediaIframe { video_type, .. })
            | BcMedia::Pframe(BcMediaPframe { video_type, .. }) => {
                self.vid_type = Some(*video_type);
            }
        }
    }
}

pub(super) async fn make_dummy_factory(
    use_splash: bool,
    pattern: String,
) -> AnyResult<NeoMediaFactory> {
    NeoMediaFactory::new_with_callback(move |element| {
        clear_bin(&element)?;
        if !use_splash {
            Ok(None)
        } else {
            build_unknown(&element, &pattern)?;
            Ok(Some(element))
        }
    })
    .await
}

enum ClientMsg {
    NewClient {
        element: Element,
        reply: std::sync::mpsc::SyncSender<Element>,
    },
}

/// Covers a relay reconnect over 4G, login and the first frames.
const CLIENT_SETUP_TIMEOUT: Duration = Duration::from_secs(30);
/// How often the last keyframe is repeated while a sleeping camera wakes up.
const KEYFRAME_FILL_INTERVAL: Duration = Duration::from_millis(500);

/// Silent audio frames needed before the AAC decoder emits samples.
const AUDIO_PRIMING_FRAMES: usize = 4;

/// What a previous client learned about the stream, so the next one can be answered at once.
#[derive(Clone)]
struct StreamCache {
    config: StreamConfig,
    keyframe: Option<BcMedia>,
    audio_frame: Option<BcMedia>,
}

fn is_audio(media: &BcMedia) -> bool {
    matches!(media, BcMedia::Aac(_) | BcMedia::Adpcm(_))
}

/// Builds a silent AAC frame with the ADTS header of a frame from the camera.
///
/// The payloads are the silent raw data blocks FFmpeg's encoder emits for AAC-LC.
fn silent_aac(template: &BcMedia) -> Option<BcMedia> {
    let BcMedia::Aac(aac) = template else {
        return None;
    };
    let header = aac.data.get(..7)?;
    if header[0] != 0xff || header[1] & 0xf0 != 0xf0 {
        return None;
    }
    let channels = ((header[2] & 0b1) << 2) | (header[3] >> 6);
    let payload: &[u8] = match channels {
        1 => &[0x01, 0x18, 0x20, 0x07],
        2 => &[0x21, 0x10, 0x04, 0x60, 0x8c, 0x1c],
        _ => return None,
    };
    let len = header.len() + payload.len();
    let mut data = header.to_vec();
    data[1] |= 0b1; // no CRC, so the header stays 7 bytes
    data[3] = (data[3] & 0b1111_1100) | ((len >> 11) & 0b11) as u8;
    data[4] = ((len >> 3) & 0xff) as u8;
    data[5] = (data[5] & 0b0001_1111) | (((len & 0b111) as u8) << 5);
    data[6] &= 0b1111_1100; // a single raw data block
    data.extend_from_slice(payload);
    Some(BcMedia::Aac(BcMediaAac { data }))
}

type SharedCache = Arc<Mutex<Option<StreamCache>>>;

pub(super) async fn make_factory(
    camera: NeoInstance,
    stream: StreamKind,
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let (client_tx, mut client_rx) = mpsc(100);
    let cache: SharedCache = Default::default();

    // Learn the stream once up front so even the first client after start-up is answered at once
    let warm_camera = camera.clone();
    let warm_cache = cache.clone();
    tokio::task::spawn(async move {
        let name = warm_camera.config().await?.borrow().name.clone();
        match learn_stream(&warm_camera, stream, &name).await {
            Ok((config, buffer, _media_rx)) => {
                let mut cache = warm_cache.lock().unwrap();
                if cache.is_none() {
                    cache.replace(StreamCache {
                        config,
                        keyframe: buffer
                            .iter()
                            .find(|m| matches!(m, BcMedia::Iframe(_)))
                            .cloned(),
                        audio_frame: buffer.iter().find(|m| is_audio(m)).cloned(),
                    });
                }
            }
            Err(e) => log::info!("{name}::{stream}: Could not learn the stream type yet: {e:?}"),
        }
        AnyResult::Ok(())
    });
    // Create the task that creates the pipelines
    let thread = tokio::task::spawn(async move {
        let name = camera.config().await?.borrow().name.clone();

        while let Some(msg) = client_rx.recv().await {
            match msg {
                ClientMsg::NewClient { element, reply } => {
                    log::debug!("New client for {name}::{stream}");
                    let camera = camera.clone();
                    let name = name.clone();
                    let cache = cache.clone();
                    tokio::task::spawn(async move {
                        let r = serve_client(camera, stream, &name, element, reply, cache).await;
                        if let Err(e) = &r {
                            log::warn!(
                                "{name}::{stream}: Could not start the client stream: {e:?}"
                            );
                        }
                        r
                    });
                }
            }
        }
        AnyResult::Ok(())
    });

    // Now setup the factory
    let factory = NeoMediaFactory::new_with_callback(move |element| {
        let (reply, new_element) = std::sync::mpsc::sync_channel(1);
        client_tx.blocking_send(ClientMsg::NewClient { element, reply })?;

        // This runs on the GLib main loop that serves every RTSP client, so an unbounded
        // wait on a stalled camera freezes the whole server.
        let element = new_element
            .recv_timeout(CLIENT_SETUP_TIMEOUT + Duration::from_secs(5))
            .with_context(|| "Timed out waiting for the camera stream")?;
        Ok(Some(element))
    })
    .await?;
    Ok((factory, thread))
}

/// Learns the stream type from the first frames of a freshly started camera stream.
async fn learn_stream(
    camera: &NeoInstance,
    stream: StreamKind,
    name: &str,
) -> AnyResult<(
    StreamConfig,
    Vec<BcMedia>,
    tokio::sync::mpsc::Receiver<BcMedia>,
)> {
    timeout(CLIENT_SETUP_TIMEOUT, async {
        let mut media_rx = camera.stream_while_live(stream).await?;
        log::debug!("{name}::{stream}: Learning camera stream type");
        let mut buffer = vec![];
        let mut frame_count = 0usize;
        let mut stream_config = StreamConfig::new(camera, stream).await?;
        while let Some(media) = media_rx.recv().await {
            stream_config.update_from_media(&media);
            buffer.push(media);
            if frame_count > 10
                || (stream_config.vid_type.is_some() && stream_config.aud_type.is_some())
            {
                break;
            }
            frame_count += 1;
        }
        AnyResult::Ok((stream_config, buffer, media_rx))
    })
    .await
    .map_err(|_| anyhow!("camera did not deliver the stream within {CLIENT_SETUP_TIMEOUT:?}"))?
}

async fn serve_client(
    camera: NeoInstance,
    stream: StreamKind,
    name: &str,
    element: Element,
    reply: std::sync::mpsc::SyncSender<Element>,
    cache: SharedCache,
) -> AnyResult<()> {
    clear_bin(&element)?;
    let config = camera.config().await?.borrow().clone();

    // A sleeping 4G camera needs ~10 s to wake up, longer than clients such as Home Assistant
    // (5 s socket timeout) wait for DESCRIBE. With a known stream type the pipeline is handed
    // over at once and the last keyframe stands in until live frames arrive.
    let cached = cache.lock().unwrap().clone();
    let (stream_config, stand_in, learned) = match cached {
        Some(cached) => {
            log::debug!("{name}::{stream}: Answering from the cached stream type");
            (cached.config, (cached.keyframe, cached.audio_frame), None)
        }
        None => {
            log::debug!("{name}::{stream}: Starting camera");
            let (stream_config, buffer, media_rx) = learn_stream(&camera, stream, name).await?;
            let keyframe = buffer
                .iter()
                .find(|m| matches!(m, BcMedia::Iframe(_)))
                .cloned();
            let audio_frame = buffer.iter().find(|m| is_audio(m)).cloned();
            cache.lock().unwrap().replace(StreamCache {
                config: stream_config.clone(),
                keyframe,
                audio_frame,
            });
            (stream_config, (None, None), Some((buffer, media_rx)))
        }
    };

    log::trace!("{name}::{stream}: Building the pipeline");
    // Build the right video pipeline
    let vid_src = match stream_config.vid_type.as_ref() {
        Some(VideoType::H264) => Some(build_h264(&element, &stream_config)?),
        Some(VideoType::H265) => Some(build_h265(&element, &stream_config)?),
        None => {
            build_unknown(&element, &config.splash_pattern.to_string())?;
            None
        }
    };

    // Build the right audio pipeline
    let aud_src = match stream_config.aud_type.as_ref() {
        Some(AudioType::Aac) => Some(build_aac(&element, &stream_config)?),
        Some(AudioType::Adpcm(block_size)) => {
            Some(build_adpcm(&element, *block_size, &stream_config)?)
        }
        None => None,
    };

    for app in vid_src.iter().chain(aud_src.iter()) {
        app.set_callbacks(
            AppSrcCallbacks::builder()
                .seek_data(move |_, _seek_pos| true)
                .build(),
        );
    }

    log::trace!("{name}::{stream}: Sending pipeline to gstreamer");
    // Send the pipeline back to the factory so it can start
    if reply.send(element).is_err() {
        log::warn!("{name}::{stream}: Client gave up before the stream was ready");
        return Ok(());
    }

    let (frame_tx, frame_rx) = std::sync::mpsc::channel();
    let forward_name = name.to_string();
    tokio::task::spawn(async move {
        let mut media_rx = match learned {
            Some((buffer, media_rx)) => {
                for media in buffer {
                    if frame_tx.send(media).is_err() {
                        return;
                    }
                }
                media_rx
            }
            None => match timeout(CLIENT_SETUP_TIMEOUT, camera.stream_while_live(stream)).await {
                Ok(Ok(media_rx)) => media_rx,
                Ok(Err(e)) => {
                    log::warn!("{forward_name}::{stream}: Could not wake the camera: {e:?}");
                    return;
                }
                Err(_) => {
                    log::warn!(
                        "{forward_name}::{stream}: Camera did not wake within {CLIENT_SETUP_TIMEOUT:?}"
                    );
                    return;
                }
            },
        };
        while let Some(media) = media_rx.recv().await {
            if let Some(cached) = cache.lock().unwrap().as_mut() {
                if matches!(media, BcMedia::Iframe(_)) {
                    cached.keyframe = Some(media.clone());
                } else if is_audio(&media) {
                    cached.audio_frame = Some(media.clone());
                }
            }
            if frame_tx.send(media).is_err() {
                break;
            }
        }
    });

    // Run blocking code on a seperate thread
    // This is not an async thread
    let name = name.to_string();
    std::thread::spawn(move || {
        let mut aud_ts = 0u32;
        let mut vid_ts = 0u32;
        let mut pools = Default::default();
        let mut live = false;
        let (keyframe, audio_frame) = stand_in;

        // GStreamer only answers DESCRIBE, and again PLAY after its flushing seek, once every
        // stream has carried data through parser and decoder, so audio is fed with silence
        // that keeps pace with the stand-in keyframes until the camera is live
        let silence = audio_frame.as_ref().and_then(silent_aac).or(audio_frame);
        let primers = keyframe
            .iter()
            .cloned()
            .chain(std::iter::repeat_n(silence.clone(), AUDIO_PRIMING_FRAMES).flatten());
        for media in primers {
            send_to_sources(
                media,
                &mut pools,
                &vid_src,
                &aud_src,
                &mut vid_ts,
                &mut aud_ts,
                &stream_config,
            )?;
        }

        log::trace!("{name}::{stream}: Sending frames");
        loop {
            let data = match frame_rx.recv_timeout(KEYFRAME_FILL_INTERVAL) {
                Ok(data) => {
                    if !live {
                        live = true;
                        // Keep audio in step with the video time spent on stand-in keyframes
                        aud_ts = vid_ts;
                    }
                    data
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => match (&keyframe, live) {
                    (Some(keyframe), false) => {
                        // Stand-ins go out every KEYFRAME_FILL_INTERVAL; stamping them one frame
                        // apart would fall behind the clock and get dropped as late
                        vid_ts += (KEYFRAME_FILL_INTERVAL.as_micros() as u32)
                            .saturating_sub(1_000_000 / stream_config.fps.max(1));
                        if let Some(silence) = &silence {
                            while aud_ts < vid_ts {
                                send_to_sources(
                                    silence.clone(),
                                    &mut pools,
                                    &vid_src,
                                    &aud_src,
                                    &mut vid_ts,
                                    &mut aud_ts,
                                    &stream_config,
                                )?;
                            }
                        }
                        keyframe.clone()
                    }
                    _ => continue,
                },
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            };
            let r = send_to_sources(
                data,
                &mut pools,
                &vid_src,
                &aud_src,
                &mut vid_ts,
                &mut aud_ts,
                &stream_config,
            );
            if let Err(r) = &r {
                log::info!("Failed to send to source: {r:?}");
            }
            r?;
        }
        log::trace!("All media recieved");
        AnyResult::Ok(())
    });
    Ok(())
}

fn send_to_sources(
    data: BcMedia,
    pools: &mut HashMap<usize, gstreamer::BufferPool>,
    vid_src: &Option<AppSrc>,
    aud_src: &Option<AppSrc>,
    vid_ts: &mut u32,
    aud_ts: &mut u32,
    stream_config: &StreamConfig,
) -> AnyResult<()> {
    // Update TS
    match data {
        BcMedia::Aac(aac) => {
            let duration = aac.duration().expect("Could not calculate AAC duration");
            if let Some(aud_src) = aud_src.as_ref() {
                log::debug!("Sending AAC: {:?}", Duration::from_micros(*aud_ts as u64));
                send_to_appsrc(
                    aud_src,
                    aac.data,
                    Duration::from_micros(*aud_ts as u64),
                    pools,
                )?;
            }
            *aud_ts += duration;
        }
        BcMedia::Adpcm(adpcm) => {
            let duration = adpcm
                .duration()
                .expect("Could not calculate ADPCM duration");
            if let Some(aud_src) = aud_src.as_ref() {
                log::trace!("Sending ADPCM: {:?}", Duration::from_micros(*aud_ts as u64));
                send_to_appsrc(
                    aud_src,
                    adpcm.data,
                    Duration::from_micros(*aud_ts as u64),
                    pools,
                )?;
            }
            *aud_ts += duration;
        }
        BcMedia::Iframe(BcMediaIframe { data, .. })
        | BcMedia::Pframe(BcMediaPframe { data, .. }) => {
            if let Some(vid_src) = vid_src.as_ref() {
                log::trace!("Sending VID: {:?}", Duration::from_micros(*vid_ts as u64));
                send_to_appsrc(vid_src, data, Duration::from_micros(*vid_ts as u64), pools)?;
            }
            const MICROSECONDS: u32 = 1000000;
            *vid_ts += MICROSECONDS / stream_config.fps;
        }
        _ => {}
    }
    Ok(())
}

fn send_to_appsrc(
    appsrc: &AppSrc,
    data: Vec<u8>,
    mut ts: Duration,
    pools: &mut HashMap<usize, gstreamer::BufferPool>,
) -> AnyResult<()> {
    check_live(appsrc)?; // Stop if appsrc is dropped

    // In live mode we follow the advice in
    // https://gstreamer.freedesktop.org/documentation/additional/design/element-source.html?gi-language=c#live-sources
    // Only push buffers when in play state and have a clock
    // we also timestamp at the current time
    if appsrc.is_live() {
        if let Some(time) = appsrc
            .current_clock_time()
            .and_then(|t| appsrc.base_time().map(|bt| t - bt))
        {
            if matches!(appsrc.current_state(), gstreamer::State::Playing) {
                ts = Duration::from_micros(time.useconds());
            } else {
                // Not playing
                return Ok(());
            }
        } else {
            // Clock not up yet
            return Ok(());
        }
    }
    let buf = {
        let msg_size = data.len();

        // Get or create a pool of this len
        let pool = pools.entry(msg_size).or_insert_with_key(|size| {
            let pool = gstreamer::BufferPool::new();
            let mut pool_config = pool.config();
            // Set a max buffers to ensure we don't grow in memory endlessly
            pool_config.set_params(None, (*size) as u32, 8, 32);
            pool.set_config(pool_config).unwrap();
            pool.set_active(true).unwrap();
            pool
        });

        // Get a buffer from the pool and then copy in the data
        let gst_buf = {
            let mut new_buf = pool.acquire_buffer(None).unwrap();
            let gst_buf_mut = new_buf.get_mut().unwrap();
            let time = ClockTime::from_useconds(ts.as_micros() as u64);
            gst_buf_mut.set_dts(time);
            gst_buf_mut.set_pts(time);
            let mut gst_buf_data = gst_buf_mut.map_writable().unwrap();
            gst_buf_data.copy_from_slice(data.as_slice());
            drop(gst_buf_data);
            new_buf
        };

        // Return the new buffer with the data
        gst_buf
    };

    // Push buffer into the appsrc
    match appsrc.push_buffer(buf) {
        Ok(_) => {
            // log::info!(
            //     "Send {}{} on {}",
            //     data.data.len(),
            //     if data.keyframe { " (keyframe)" } else { "" },
            //     appsrc.name()
            // );
            Ok(())
        }
        Err(FlowError::Flushing) => {
            // Buffer is full just skip
            log::info!(
                "Buffer full on {} pausing stream until client consumes frames",
                appsrc.name()
            );
            Ok(())
        }
        Err(e) => Err(anyhow!("Error in streaming: {e:?}")),
    }?;
    // Check if we need to pause
    if appsrc.current_level_bytes() >= appsrc.max_bytes() * 2 / 3
        && matches!(appsrc.current_state(), gstreamer::State::Paused)
    {
        appsrc.set_state(gstreamer::State::Playing).unwrap();
    } else if appsrc.current_level_bytes() <= appsrc.max_bytes() / 3
        && matches!(appsrc.current_state(), gstreamer::State::Playing)
    {
        appsrc.set_state(gstreamer::State::Paused).unwrap();
    }
    Ok(())
}
fn check_live(app: &AppSrc) -> Result<()> {
    app.bus().ok_or(anyhow!("App source is closed"))?;
    app.pads()
        .iter()
        .all(|pad| pad.is_linked())
        .then_some(())
        .ok_or(anyhow!("App source is not linked"))
}

fn clear_bin(bin: &Element) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    // Clear the autogenerated ones
    for element in bin.iterate_elements().into_iter().flatten() {
        bin.remove(&element)?;
    }

    Ok(())
}

fn build_unknown(bin: &Element, pattern: &str) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Unknown Pipeline");
    let source = make_element("videotestsrc", "testvidsrc")?;
    source.set_property_from_str("pattern", pattern);
    source.set_property("num-buffers", 500i32); // Send buffers then EOS
    let queue = make_queue("queue0", 1024 * 1024 * 4)?;

    let overlay = make_element("textoverlay", "overlay")?;
    overlay.set_property("text", "Stream not Ready");
    overlay.set_property_from_str("valignment", "top");
    overlay.set_property_from_str("halignment", "left");
    overlay.set_property("font-desc", "Sans, 16");
    let encoder = make_element("jpegenc", "encoder")?;
    let payload = make_element("rtpjpegpay", "pay0")?;

    bin.add_many([&source, &queue, &overlay, &encoder, &payload])?;
    source.link_filtered(
        &queue,
        &Caps::builder("video/x-raw")
            .field("format", "YUY2")
            .field("width", 896i32)
            .field("height", 512i32)
            .field("framerate", gstreamer::Fraction::new(25, 1))
            .build(),
    )?;
    Element::link_many([&queue, &overlay, &encoder, &payload])?;

    Ok(())
}

struct Linked {
    appsrc: AppSrc,
    output: Element,
}

fn pipe_h264(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    log::debug!(
        "buffer_size: {buffer_size}, bitrate: {}",
        stream_config.bitrate
    );
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H264 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h264parse", "parser")?;
    // let stamper = make_element("h264timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h264(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h264(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph264pay", "pay0")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_h265(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H265 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h265parse", "parser")?;
    // let stamper = make_element("h265timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h265(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h265(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph265pay", "pay0")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_aac(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Aac pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let parser = make_element("aacparse", "audparser")?;
    let decoder = match make_element("faad", "auddecoder_faad") {
        Ok(ele) => Ok(ele),
        Err(_) => make_element("avdec_aac", "auddecoder_avdec_aac"),
    }?;

    // The fallback
    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let fallback_switch = make_element("fallbackswitch", "audfallbackswitch");
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        fallback_switch.set_property("timeout", 3u64 * 1_000_000_000u64);
        fallback_switch.set_property("immediate-fallback", true);
    }

    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &queue, &parser, &decoder, &encoder])?;
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        bin.add_many([&silence, fallback_switch])?;
        Element::link_many([
            &source,
            &queue,
            &parser,
            &decoder,
            fallback_switch,
            &encoder,
        ])?;
        Element::link_many([&silence, fallback_switch])?;
    } else {
        Element::link_many([&source, &queue, &parser, &decoder, &encoder])?;
    }

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

fn build_aac(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_aac(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Adpcm pipeline");
    // Original command line
    // caps=audio/x-adpcm,layout=dvi,block_align={},channels=1,rate=8000
    // ! queue silent=true max-size-bytes=10485760 min-threshold-bytes=1024
    // ! adpcmdec
    // ! audioconvert
    // ! rtpL16pay name=pay1

    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    source.set_caps(Some(
        &Caps::builder("audio/x-adpcm")
            .field("layout", "div")
            .field("block_align", block_size as i32)
            .field("channels", 1i32)
            .field("rate", 8000i32)
            .build(),
    ));

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let decoder = make_element("decodebin", "auddecoder")?;
    let encoder = make_element("audioconvert", "audencoder")?;
    let encoder_out = encoder.clone();

    bin.add_many([&source, &queue, &decoder, &encoder])?;
    Element::link_many([&source, &queue, &decoder])?;
    decoder.connect_pad_added(move |_element, pad| {
        let sink_pad = encoder
            .static_pad("sink")
            .expect("Encoder is missing its pad");
        pad.link(&sink_pad)
            .expect("Failed to link ADPCM decoder to encoder");
    });

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder_out,
    })
}

fn build_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_adpcm(bin, block_size, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

#[allow(dead_code)]
fn pipe_silence(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Silence pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let sink_queue = make_queue("audsinkqueue", buffer_size)?;
    let sink = make_element("fakesink", "silence_sink")?;

    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let src_queue = make_queue("audsinkqueue", buffer_size)?;
    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &sink_queue, &sink, &silence, &src_queue, &encoder])?;

    Element::link_many([&source, &sink_queue, &sink])?;

    Element::link_many([&silence, &src_queue, &encoder])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

#[allow(dead_code)]
struct AppSrcPair {
    vid: AppSrc,
    aud: Option<AppSrc>,
}

// #[allow(dead_code)]
// /// Experimental build a stream of MPEGTS
// fn build_mpegts(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrcPair> {
//     let buffer_size = buffer_size(stream_config.bitrate);
//     log::debug!(
//         "buffer_size: {buffer_size}, bitrate: {}",
//         stream_config.bitrate
//     );

//     // VID
//     let vid_link = match stream_config.vid_format {
//         VidFormat::H264 => pipe_h264(bin, stream_config)?,
//         VidFormat::H265 => pipe_h265(bin, stream_config)?,
//         VidFormat::None => unreachable!(),
//     };

//     // AUD
//     let aud_link = match stream_config.aud_format {
//         AudFormat::Aac => pipe_aac(bin, stream_config)?,
//         AudFormat::Adpcm(block) => pipe_adpcm(bin, block, stream_config)?,
//         AudFormat::None => pipe_silence(bin, stream_config)?,
//     };

//     let bin = bin
//         .clone()
//         .dynamic_cast::<Bin>()
//         .map_err(|_| anyhow!("Media source's element should be a bin"))?;

//     // MUX
//     let muxer = make_element("mpegtsmux", "mpeg_muxer")?;
//     let rtp = make_element("rtpmp2tpay", "pay0")?;

//     bin.add_many([&muxer, &rtp])?;
//     Element::link_many([&vid_link.output, &muxer, &rtp])?;
//     Element::link_many([&aud_link.output, &muxer])?;

//     Ok(AppSrcPair {
//         vid: vid_link.appsrc,
//         aud: Some(aud_link.appsrc),
//     })
// }

// Convenice funcion to make an element or provide a message
// about what plugin is missing
fn make_element(kind: &str, name: &str) -> AnyResult<Element> {
    ElementFactory::make_with_name(kind, Some(name)).with_context(|| {
        let plugin = match kind {
            "appsrc" => "app (gst-plugins-base)",
            "audioconvert" => "audioconvert (gst-plugins-base)",
            "adpcmdec" => "Required for audio",
            "h264parse" => "videoparsersbad (gst-plugins-bad)",
            "h265parse" => "videoparsersbad (gst-plugins-bad)",
            "rtph264pay" => "rtp (gst-plugins-good)",
            "rtph265pay" => "rtp (gst-plugins-good)",
            "rtpjitterbuffer" => "rtp (gst-plugins-good)",
            "aacparse" => "audioparsers (gst-plugins-good)",
            "rtpL16pay" => "rtp (gst-plugins-good)",
            "x264enc" => "x264 (gst-plugins-ugly)",
            "x265enc" => "x265 (gst-plugins-bad)",
            "avdec_h264" => "libav (gst-libav)",
            "avdec_h265" => "libav (gst-libav)",
            "videotestsrc" => "videotestsrc (gst-plugins-base)",
            "imagefreeze" => "imagefreeze (gst-plugins-good)",
            "audiotestsrc" => "audiotestsrc (gst-plugins-base)",
            "decodebin" => "playback (gst-plugins-good)",
            _ => "Unknown",
        };
        format!(
            "Missing required gstreamer plugin `{}` for `{}` element",
            plugin, kind
        )
    })
}

#[allow(dead_code)]
fn make_dbl_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    // queue.set_property(
    //     "max-size-time",
    //     std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
    //         .unwrap_or(0),
    // );

    let queue2 = make_element("queue2", &format!("queue2_{}", name))?;
    queue2.set_property("max-size-bytes", buffer_size * 2u32 / 3u32);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue2.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    queue2.set_property("use-buffering", false);

    let bin = gstreamer::Bin::builder().name(name).build();
    bin.add_many([&queue, &queue2])?;
    Element::link_many([&queue, &queue2])?;

    let pad = queue
        .static_pad("sink")
        .expect("Failed to get a static pad from queue.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let pad = queue2
        .static_pad("src")
        .expect("Failed to get a static pad from queue2.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let bin = bin
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot convert bin"))?;
    Ok(bin)
}

fn make_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    Ok(queue)
}

fn buffer_size(bitrate: u32) -> u32 {
    // 0.1 seconds (according to bitrate) or 4kb what ever is larger
    std::cmp::max(bitrate * 2 / 8u32, 4u32 * 1024u32)
}
