//! gRPC control surface for haze-media (`haze.media.v1.MediaControlService`).
//!
//! It serves the same three operations as the HTTP routes, and shares their
//! code: audio streaming goes through [`AudioOut`] and WebRTC offers through
//! [`prepare_webrtc_offer`]. The HTTP routes stay the reference behavior, and
//! this listener is an alternative transport selected by `haze-web`
//! (`docs/grpc-migration-plan.md`, Phase 1).

use std::io;
use std::time::Duration;

use anyhow::{Context, Result};
use haze_contracts::media::v1::media_control_service_server::{
    MediaControlService, MediaControlServiceServer,
};
use haze_contracts::media::v1::{
    HealthRequest, HealthResponse, StreamFeedAudioRequest, StreamFeedAudioResponse,
    WebRtcOfferRequest, WebRtcOfferResponse,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use super::{
    prepare_webrtc_offer, start_webrtc_peer_feeder, stream_feed_audio, valid_feed_id, AudioOut,
    MediaState, OfferRefusal, WebRTCOfferRequest as OfferPayload,
};

/// Messages buffered between the audio writer and the gRPC stream. The writer
/// waits when the listener is slow, which is what lets the broadcast channel
/// notice the lag and resynchronize instead of queueing stale audio.
const AUDIO_QUEUE_MESSAGES: usize = 8;
/// Largest request the server accepts. An SDP offer is limited to 512 KiB.
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Sends audio to a gRPC stream. The content type travels on the first message.
pub(crate) struct AudioSender {
    tx: mpsc::Sender<std::result::Result<StreamFeedAudioResponse, Status>>,
    content_type: Option<String>,
}

impl AudioSender {
    fn new(tx: mpsc::Sender<std::result::Result<StreamFeedAudioResponse, Status>>) -> Self {
        Self {
            tx,
            content_type: None,
        }
    }

    pub(crate) fn start(&mut self, content_type: &str) {
        self.content_type = Some(content_type.to_string());
    }

    pub(crate) async fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        let message = StreamFeedAudioResponse {
            content_type: self.content_type.take().unwrap_or_default(),
            data: bytes.to_vec(),
        };
        self.tx
            .send(Ok(message))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
    }

    pub(crate) async fn refuse(&mut self, status: u16, detail: &str) -> Result<()> {
        // A closed channel means the listener already left, so there is nobody to tell.
        let _ = self.tx.send(Err(status_for_http(status, detail))).await;
        Ok(())
    }
}

/// Maps an HTTP status used by the HTTP routes to the matching gRPC status.
fn status_for_http(status: u16, detail: &str) -> Status {
    let message = detail.trim();
    match status {
        400 => Status::invalid_argument(message),
        404 => Status::not_found(message),
        415 => Status::unimplemented(message),
        429 => Status::resource_exhausted(message),
        503 => Status::unavailable(message),
        _ => Status::internal(message),
    }
}

fn status_for_refusal(refusal: &OfferRefusal) -> Status {
    status_for_http(refusal.status, &refusal.message())
}

#[derive(Clone)]
struct MediaControl {
    state: MediaState,
}

#[tonic::async_trait]
impl MediaControlService for MediaControl {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> std::result::Result<Response<HealthResponse>, Status> {
        let status_json = serde_json::to_vec(&self.state.health())
            .map_err(|err| Status::internal(format!("failed to encode health: {err}")))?;
        Ok(Response::new(HealthResponse { status_json }))
    }

    type StreamFeedAudioStream =
        ReceiverStream<std::result::Result<StreamFeedAudioResponse, Status>>;

    async fn stream_feed_audio(
        &self,
        request: Request<StreamFeedAudioRequest>,
    ) -> std::result::Result<Response<Self::StreamFeedAudioStream>, Status> {
        let request = request.into_inner();
        if !valid_feed_id(&request.feed_id) {
            return Err(Status::invalid_argument("feed is required"));
        }
        let (tx, rx) = mpsc::channel(AUDIO_QUEUE_MESSAGES);
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut sink = AudioOut::Grpc(AudioSender::new(tx));
            if let Err(err) =
                stream_feed_audio(&mut sink, &state, &request.feed_id, &request.codec).await
            {
                debug!("haze-media gRPC audio stream ended: {err:#}");
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn web_rtc_offer(
        &self,
        request: Request<WebRtcOfferRequest>,
    ) -> std::result::Result<Response<WebRtcOfferResponse>, Status> {
        let remote_addr = request.remote_addr();
        let message = request.into_inner();
        let payload = OfferPayload {
            feed_id: message.feed_id,
            sdp: message.sdp,
            _sdp_type: message.sdp_type,
            preferred_codec: message.preferred_codec,
            codec: message.codec,
            require_opus: message.require_opus,
            disable_g722: message.disable_g722,
            client_ip: message.client_ip,
            remote_addr: message.remote_addr,
        };
        let accepted = prepare_webrtc_offer(&self.state, &payload, remote_addr)
            .await
            .map_err(|refusal| status_for_refusal(&refusal))?;
        let response = WebRtcOfferResponse {
            feed_id: accepted.feed_id.clone(),
            sdp: accepted.answer_sdp.clone(),
            sdp_type: "answer".to_string(),
            codec: accepted.codec.to_string(),
            payload_type: u32::from(accepted.payload_type),
            media_recent: accepted.media_recent,
        };
        start_webrtc_peer_feeder(
            self.state.clone(),
            accepted.peer_id,
            accepted.feed,
            accepted.peer,
        );
        Ok(Response::new(response))
    }
}

/// Serves `MediaControlService` on an already bound listener until it fails.
async fn serve(listener: TcpListener, state: MediaState) -> Result<()> {
    let service = MediaControlServiceServer::new(MediaControl { state })
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    Server::builder()
        .tcp_nodelay(true)
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .add_service(service)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .context("haze-media gRPC server stopped")
}

/// Binds and serves the gRPC control listener. A failure is logged and ends only
/// this listener, because the HTTP routes carry the live audio and must keep
/// running either way.
pub(crate) async fn run_grpc_server(addr: String, state: MediaState) {
    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(err) => {
            error!("failed to bind the haze-media gRPC listener at {addr}: {err}");
            return;
        }
    };
    match listener.local_addr() {
        Ok(local) => {
            if !local.ip().is_loopback() {
                warn!(
                    %local,
                    "the haze-media gRPC listener has no authentication and should stay on a loopback address"
                );
            }
            info!(%local, "haze-media gRPC control listening");
        }
        Err(err) => warn!("could not read the haze-media gRPC listener address: {err}"),
    }
    if let Err(err) = serve(listener, state).await {
        error!("{err:#}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use haze_contracts::media::v1::media_control_service_client::MediaControlServiceClient;
    use tokio_stream::StreamExt;
    use tonic::Code;

    use super::super::{
        output_feed_from_node, BackendMode, OutputsXml, WebRTCAudioCodec, FRAME_BYTES,
    };

    fn state(feed_id: &str, webrtc_codecs: Vec<WebRTCAudioCodec>) -> MediaState {
        let xml =
            format!(r#"<outputs><feed id="{feed_id}"><webrtc enabled="true"/></feed></outputs>"#);
        let parsed: OutputsXml = quick_xml::de::from_str(&xml).expect("outputs XML");
        let feed = parsed.feeds.into_iter().next().expect("one feed");
        let output = output_feed_from_node(feed.id.trim(), feed.icecast, feed.webrtc, true);
        // Pretending GStreamer is present lets offers reach validation without a real encoder.
        MediaState::new(
            BTreeMap::from([(feed.id, output)]),
            BackendMode::Legacy,
            !webrtc_codecs.is_empty(),
            webrtc_codecs,
        )
    }

    async fn client(state: MediaState) -> MediaControlServiceClient<tonic::transport::Channel> {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("address");
        tokio::spawn(serve(listener, state));
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .expect("endpoint")
            .connect()
            .await
            .expect("connect to the gRPC listener");
        MediaControlServiceClient::new(channel)
    }

    fn offer(feed_id: &str, sdp: &str) -> WebRtcOfferRequest {
        WebRtcOfferRequest {
            feed_id: feed_id.to_string(),
            sdp: sdp.to_string(),
            ..WebRtcOfferRequest::default()
        }
    }

    const OPUS_OFFER: &str =
        "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\n";

    #[test]
    fn http_statuses_map_to_the_documented_grpc_codes() {
        let cases = [
            (400, Code::InvalidArgument),
            (404, Code::NotFound),
            (415, Code::Unimplemented),
            (429, Code::ResourceExhausted),
            (503, Code::Unavailable),
            (500, Code::Internal),
            (418, Code::Internal),
        ];
        for (http, grpc) in cases {
            assert_eq!(
                status_for_http(http, "detail\n").code(),
                grpc,
                "HTTP {http}"
            );
        }
        assert_eq!(status_for_http(404, " not here\n").message(), "not here");
    }

    #[tokio::test]
    async fn health_returns_the_same_document_as_the_http_route() {
        let state = state("sk-0001", Vec::new());
        let mut client = client(state.clone()).await;
        let response = client
            .health(HealthRequest {})
            .await
            .expect("health")
            .into_inner();
        let health: serde_json::Value =
            serde_json::from_slice(&response.status_json).expect("health JSON");
        assert_eq!(health["ok"], true);
        assert_eq!(health["service"], "haze-media");
        assert_eq!(health["capabilities"]["http_audio"], true);
        assert_eq!(health["http_client_count"], 0);
    }

    #[tokio::test]
    async fn audio_requests_are_refused_with_the_matching_codes() {
        let mut client = client(state("sk-0001", Vec::new())).await;

        let err = client
            .stream_feed_audio(StreamFeedAudioRequest {
                feed_id: "not a feed".to_string(),
                codec: String::new(),
            })
            .await
            .expect_err("invalid feed id");
        assert_eq!(err.code(), Code::InvalidArgument);

        // A refusal that happens after the stream opened arrives on the stream.
        for (feed, codec, expected) in [
            ("missing-feed", "pcm16", Code::NotFound),
            ("sk-0001", "flac", Code::Unimplemented),
            ("sk-0001", "opus", Code::Unavailable),
        ] {
            let mut stream = client
                .stream_feed_audio(StreamFeedAudioRequest {
                    feed_id: feed.to_string(),
                    codec: codec.to_string(),
                })
                .await
                .expect("stream opens")
                .into_inner();
            let err = stream
                .next()
                .await
                .expect("the refusal is delivered")
                .expect_err("refused");
            assert_eq!(err.code(), expected, "{feed} {codec}");
        }
    }

    #[tokio::test]
    async fn wav_stream_starts_with_content_type_header_and_silence_then_cleans_up() {
        let state = state("sk-0001", Vec::new());
        let mut client = client(state.clone()).await;
        let mut stream = client
            .stream_feed_audio(StreamFeedAudioRequest {
                feed_id: "sk-0001".to_string(),
                codec: String::new(),
            })
            .await
            .expect("stream opens")
            .into_inner();

        let first = stream.next().await.expect("first message").expect("ok");
        assert_eq!(first.content_type, "audio/wav");
        assert_eq!(&first.data[..4], b"RIFF");
        assert_eq!(first.data.len(), 44);
        let second = stream.next().await.expect("silence").expect("ok");
        assert!(second.content_type.is_empty(), "content type is sent once");
        assert_eq!(second.data, vec![0u8; FRAME_BYTES]);
        // The feed clock keeps producing paced frames.
        let third = stream.next().await.expect("paced frame").expect("ok");
        assert_eq!(third.data.len(), FRAME_BYTES);

        assert_eq!(state.health()["http_client_count"], 1);
        drop(stream);
        for _ in 0..100 {
            if state.health()["http_client_count"] == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the listener was not unregistered after the client cancelled");
    }

    #[tokio::test]
    async fn raw_stream_has_no_wav_header() {
        let mut client = client(state("sk-0001", Vec::new())).await;
        let mut stream = client
            .stream_feed_audio(StreamFeedAudioRequest {
                feed_id: "sk-0001".to_string(),
                codec: "raw".to_string(),
            })
            .await
            .expect("stream opens")
            .into_inner();
        let first = stream.next().await.expect("first message").expect("ok");
        assert_eq!(first.content_type, "audio/L16; rate=48000; channels=1");
        assert_eq!(first.data, vec![0u8; FRAME_BYTES]);
    }

    #[tokio::test]
    async fn offers_are_refused_with_the_same_rules_as_the_http_route() {
        // WebRTC backend unavailable.
        let mut unavailable = client(state("sk-0001", Vec::new())).await;
        let err = unavailable
            .web_rtc_offer(offer("sk-0001", OPUS_OFFER))
            .await
            .expect_err("backend unavailable");
        assert_eq!(err.code(), Code::Unavailable);
        assert_eq!(err.message(), "str0m WebRTC backend is unavailable");

        let mut client = client(state("sk-0001", vec![WebRTCAudioCodec::Opus])).await;
        let err = client
            .web_rtc_offer(offer("", OPUS_OFFER))
            .await
            .expect_err("missing feed id");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert_eq!(err.message(), "feed_id is required");

        let err = client
            .web_rtc_offer(offer("sk-0001", "  "))
            .await
            .expect_err("missing sdp");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert_eq!(err.message(), "sdp is required");

        let err = client
            .web_rtc_offer(offer("sk-0001", "v=0\r\n"))
            .await
            .expect_err("no usable codec");
        assert_eq!(err.code(), Code::Unimplemented);

        let err = client
            .web_rtc_offer(offer("other-feed", OPUS_OFFER))
            .await
            .expect_err("unknown feed");
        assert_eq!(err.code(), Code::NotFound);
        assert_eq!(err.message(), "feed is not configured");
    }
}
