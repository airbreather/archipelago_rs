use crate::protocol::*;
use bytes::BytesMut;
use ratchet_core::{Receiver, Sender, WebSocketStream};
use ratchet_rs::deflate::{Deflate, DeflateDecoder, DeflateEncoder, DeflateExtProvider};
use ratchet_rs::{
    subscribe_with, ExtensionDecoder, Message, SubprotocolRegistry, UpgradedClient, WebSocketConfig,
};
use std::pin::Pin;
use std::str::{from_utf8, Utf8Error};
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter, ReadBuf};
use tokio::net::TcpStream;
use tokio_native_tls::native_tls::TlsConnector;
use tokio_native_tls::TlsStream;

#[derive(Error, Debug)]
pub enum ArchipelagoError {
    #[error("illegal response")]
    IllegalResponse {
        received: ServerMessage,
        expected: &'static str,
    },
    #[error("connection closed by server")]
    ConnectionClosed,
    #[error("data failed to serialize")]
    FailedSerialize(#[from] serde_json::Error),
    #[error("unexpected non-text result from websocket")]
    NonTextWebsocketResult(Message),
    #[error("network error")]
    NetworkError(#[from] tokio::io::Error),
    #[error("websocket error")]
    WebSocketError(#[from] ratchet_rs::Error),
    #[error("server sent invalid utf-8")]
    InvalidUtf8Error(#[from] Utf8Error),
}

enum MaybeTlsStream {
    Tls(TlsStream<TcpStream>),
    Plain(TcpStream),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

async fn try_connect_inner(host: &str, port: u16) -> Result<MaybeTlsStream, ArchipelagoError> {
    // Attempt WSS, downgrade to WS if the TLS handshake fails
    let mut stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true)?;
    if let Ok(cx) = TlsConnector::builder().build() {
        let cx = tokio_native_tls::TlsConnector::from(cx);
        if let Ok(stream) = cx.connect(host, stream).await {
            return Ok(MaybeTlsStream::Tls(stream));
        }

        // even just ATTEMPTING the TLS stuff transfers ownership, and the Err branch doesn't give
        // back the original stream, so I guess we have to open this all over again?
        stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
    }

    Ok(MaybeTlsStream::Plain(stream))
}

async fn try_connect(
    host: &str,
    port: u16,
) -> Result<UpgradedClient<BufReader<BufWriter<MaybeTlsStream>>, Deflate>, ArchipelagoError> {
    let stream = try_connect_inner(host, port).await?;
    let url = format!(
        "{}{}:{}",
        match stream {
            MaybeTlsStream::Plain(_) => "ws://",
            MaybeTlsStream::Tls(_) => "wss://",
        },
        host,
        port
    );
    Ok(subscribe_with(
        WebSocketConfig::default(),
        BufReader::new(BufWriter::new(stream)),
        url,
        &DeflateExtProvider::default(),
        SubprotocolRegistry::default(),
    )
    .await?)
}

pub struct ArchipelagoClient {
    sender: ArchipelagoClientSender,
    receiver: ArchipelagoClientReceiver,
}

impl ArchipelagoClient {
    /**
     * Create an instance of the client and connect to the server on the given URL
     */
    pub async fn new(host: &str, port: u16) -> Result<ArchipelagoClient, ArchipelagoError> {
        let (sender, mut receiver) = try_connect(host, port).await?.websocket.split()?;

        let mut buf = BytesMut::new();
        let response = recv(&mut receiver, &mut buf).await?;
        let mut iter = response.into_iter();
        let room_info = match iter.next() {
            Some(ServerMessage::RoomInfo(room)) => room,
            Some(received) => {
                return Err(ArchipelagoError::IllegalResponse {
                    received,
                    expected: "Expected RoomInfo",
                })
            }
            None => return Err(ArchipelagoError::ConnectionClosed),
        };

        Ok(ArchipelagoClient {
            sender: ArchipelagoClientSender { ws: sender },
            receiver: ArchipelagoClientReceiver {
                ws: receiver,
                room_info,
                data_package: None,
                message_buffer: iter.collect(),
                buf,
            },
        })
    }

    /**
     * Create an instance of the client and connect to the server, fetching the given games' Data
     * Package
     */
    pub async fn with_data_package(
        host: &str,
        port: u16,
        mut games: Option<Vec<String>>,
    ) -> Result<ArchipelagoClient, ArchipelagoError> {
        let mut client = Self::new(host, port).await?;
        if games.is_none() {
            // If None, request the games that are part of the connected room.
            let mut list: Vec<String> = vec![];
            client
                .receiver
                .room_info
                .datapackage_checksums
                .keys()
                .for_each(|name| {
                    list.push(name.clone());
                });
            games = Some(list);
        }
        client
            .send(ClientMessage::GetDataPackage(GetDataPackage { games }))
            .await?;
        match client.recv().await? {
            ServerMessage::DataPackage(pkg) => client.receiver.data_package = Some(pkg.data),
            received => {
                return Err(ArchipelagoError::IllegalResponse {
                    received,
                    expected: "DataPackage",
                })
            }
        }

        Ok(client)
    }

    pub fn room_info(&self) -> &RoomInfo {
        &self.receiver.room_info
    }

    pub fn data_package(&self) -> Option<&DataPackageObject> {
        self.receiver.data_package.as_ref()
    }

    pub async fn send(&mut self, message: ClientMessage) -> Result<(), ArchipelagoError> {
        self.sender.send(message).await
    }

    /**
     * Read a message from the server
     *
     * Will buffer results locally, and return results from buffer or wait on network
     * if buffer is empty
     */
    pub async fn recv(&mut self) -> Result<ServerMessage, ArchipelagoError> {
        self.receiver.recv().await
    }

    /**
     * Send a connect request to the Archipelago server
     *
     * Will attempt to read a Connected packet in response, and will return an error if
     * another packet is found
     */
    pub async fn connect(
        &mut self,
        game: &str,
        name: &str,
        password: Option<&str>,
        items_handling: Option<i32>,
        tags: Vec<String>,
    ) -> Result<Connected, ArchipelagoError> {
        self.send(ClientMessage::Connect(Connect {
            game: game.to_string(),
            name: name.to_string(),
            uuid: "".to_string(),
            password: password.map(|p| p.to_string()),
            version: network_version(),
            items_handling,
            tags,
            request_slot_data: true,
        }))
        .await?;
        let response = self.recv().await?;

        match response {
            ServerMessage::Connected(connected) => Ok(connected),
            received => Err(ArchipelagoError::IllegalResponse {
                received,
                expected: "Connected",
            }),
        }
    }

    /**
     * Basic chat command which sends text to the server to be distributed to other clients.
     */
    pub async fn say(&mut self, message: &str) -> Result<(), ArchipelagoError> {
        Ok(self
            .send(ClientMessage::Say(Say {
                text: message.to_string(),
            }))
            .await?)
    }

    /**
     * Sent to server to request a ReceivedItems packet to synchronize items.
     *
     * Will buffer any non-ReceivedItems packets returned
     */
    pub async fn sync(&mut self) -> Result<ReceivedItems, ArchipelagoError> {
        self.send(ClientMessage::Sync).await?;
        let mut ignored_messages = vec![];
        let items = loop {
            match self.recv().await? {
                ServerMessage::ReceivedItems(items) => break items,
                resp => ignored_messages.push(resp),
            }
        };

        ignored_messages.reverse();
        self.receiver.message_buffer.extend(ignored_messages);
        Ok(items)
    }

    /**
     * Sent to server to inform it of locations that the client has checked.
     *
     * Used to inform the server of new checks that are made, as well as to sync state.
     */
    pub async fn location_checks(&mut self, locations: Vec<i64>) -> Result<(), ArchipelagoError> {
        Ok(self
            .send(ClientMessage::LocationChecks(LocationChecks { locations }))
            .await?)
    }

    /**
     * Sent to the server to inform it of locations the client has seen, but not checked.
     *
     * Useful in cases in which the item may appear in the game world, such as 'ledge items' in A Link to the Past. Non-LocationInfo packets will be buffered
     */
    pub async fn location_scouts(
        &mut self,
        locations: Vec<i64>,
        create_as_hint: i32,
    ) -> Result<LocationInfo, ArchipelagoError> {
        self.send(ClientMessage::LocationScouts(LocationScouts {
            locations,
            create_as_hint,
        }))
        .await?;
        let mut ignored_messages = vec![];
        let items = loop {
            match self.recv().await? {
                ServerMessage::LocationInfo(items) => break items,
                resp => ignored_messages.push(resp),
            }
        };

        ignored_messages.reverse();
        self.receiver.message_buffer.extend(ignored_messages);
        Ok(items)
    }

    /**
     * Sent to the server to update on the sender's status.
     *
     * Examples include readiness or goal completion. (Example: defeated Ganon in A Link to the Past)
     */
    pub async fn status_update(&mut self, status: ClientStatus) -> Result<(), ArchipelagoError> {
        Ok(self
            .send(ClientMessage::StatusUpdate(StatusUpdate { status }))
            .await?)
    }

    /**
     * Send this message to the server, tell it which clients should receive the message and the server will forward the message to all those targets to which any one requirement applies.
     */
    pub async fn bounce(
        &mut self,
        games: Option<Vec<String>>,
        slots: Option<Vec<String>>,
        tags: Option<Vec<String>>,
        data: serde_json::Value,
    ) -> Result<(), ArchipelagoError> {
        Ok(self
            .send(ClientMessage::Bounce(Bounce {
                games,
                slots,
                tags,
                data,
            }))
            .await?)
    }

    /**
     * Used to request a single or multiple values from the server's data storage, see the Set package for how to write values to the data storage.
     *
     * A Get package will be answered with a Retrieved package. Non-Retrieved responses are
     * buffered
     */
    pub async fn get(&mut self, keys: Vec<String>) -> Result<Retrieved, ArchipelagoError> {
        self.send(ClientMessage::Get(Get { keys })).await?;
        let mut ignored_messages = vec![];
        let items = loop {
            match self.recv().await? {
                ServerMessage::Retrieved(items) => break items,
                resp => ignored_messages.push(resp),
            }
        };

        ignored_messages.reverse();
        self.receiver.message_buffer.extend(ignored_messages);
        Ok(items)
    }

    /**
     * Used to write data to the server's data storage, that data can then be shared across worlds or just saved for later.
     *
     * Values for keys in the data storage can be retrieved with a Get package, or monitored with a SetNotify package. Non-SetReply responses are buffered
     */
    pub async fn set(
        &mut self,
        key: String,
        default: serde_json::Value,
        want_reply: bool,
        operations: Vec<DataStorageOperation>,
    ) -> Result<SetReply, ArchipelagoError> {
        self.send(ClientMessage::Set(Set {
            key,
            default,
            want_reply,
            operations,
        }))
        .await?;
        let mut ignored_messages = vec![];
        let items = loop {
            match self.recv().await? {
                ServerMessage::SetReply(items) => break items,
                resp => ignored_messages.push(resp),
            }
        };

        ignored_messages.reverse();
        self.receiver.message_buffer.extend(ignored_messages);
        Ok(items)
    }
}

/**
 * Once split, this struct handles the sending-side of your connection
 *
 * For helper method docs, see ArchipelagoClient. Helper methods that require
 * both sending and receiving are intentionally unavailable; for those messages,
 * use `send`.
 */
pub struct ArchipelagoClientSender {
    ws: Sender<BufReader<BufWriter<MaybeTlsStream>>, DeflateEncoder>,
}

impl ArchipelagoClientSender {
    pub async fn send(&mut self, message: ClientMessage) -> Result<(), ArchipelagoError> {
        self.ws
            .write_text(serde_json::to_string(&[message])?)
            .await?;
        Ok(())
    }

    pub async fn say(&mut self, message: &str) -> Result<(), ArchipelagoError> {
        Ok(self
            .send(ClientMessage::Say(Say {
                text: message.to_string(),
            }))
            .await?)
    }

    pub async fn location_checks(&mut self, locations: Vec<i64>) -> Result<(), ArchipelagoError> {
        Ok(self
            .send(ClientMessage::LocationChecks(LocationChecks { locations }))
            .await?)
    }

    pub async fn status_update(&mut self, status: ClientStatus) -> Result<(), ArchipelagoError> {
        Ok(self
            .send(ClientMessage::StatusUpdate(StatusUpdate { status }))
            .await?)
    }

    pub async fn bounce(
        &mut self,
        games: Option<Vec<String>>,
        slots: Option<Vec<String>>,
        tags: Option<Vec<String>>,
        data: serde_json::Value,
    ) -> Result<(), ArchipelagoError> {
        Ok(self
            .send(ClientMessage::Bounce(Bounce {
                games,
                slots,
                tags,
                data,
            }))
            .await?)
    }
}

/**
 * Once split, this struct handles the receiving-side of your connection
 *
 * For helper method docs, see ArchipelagoClient. Helper methods that require
 * both sending and receiving are intentionally unavailable; for those messages,
 * use `recv`.
 */
pub struct ArchipelagoClientReceiver {
    ws: Receiver<BufReader<BufWriter<MaybeTlsStream>>, DeflateDecoder>,
    room_info: RoomInfo,
    message_buffer: Vec<ServerMessage>,
    data_package: Option<DataPackageObject>,
    buf: BytesMut,
}

async fn recv<S: WebSocketStream, E: ExtensionDecoder>(
    s: &mut Receiver<S, E>,
    buf: &mut BytesMut,
) -> Result<Vec<ServerMessage>, ArchipelagoError> {
    loop {
        match s.read(buf).await {
            Ok(Message::Text) => {
                let payload = serde_json::from_str::<Vec<ServerMessage>>(from_utf8(buf.as_ref())?)?;
                buf.clear();
                break Ok(payload);
            }
            Ok(Message::Ping(_) | Message::Pong(_)) => {}
            Ok(Message::Close(_)) => break Err(ArchipelagoError::ConnectionClosed),
            Ok(msg) => break Err(ArchipelagoError::NonTextWebsocketResult(msg)),
            Err(e) => break Err(e.into()),
        }
    }
}

impl ArchipelagoClientReceiver {
    pub async fn recv(&mut self) -> Result<ServerMessage, ArchipelagoError> {
        while self.message_buffer.is_empty() {
            self.message_buffer = recv(&mut self.ws, &mut self.buf).await?;
            self.message_buffer.reverse();
        }

        Ok(self.message_buffer.pop().unwrap())
    }

    pub fn room_info(&self) -> &RoomInfo {
        &self.room_info
    }

    pub fn data_package(&self) -> Option<&DataPackageObject> {
        self.data_package.as_ref()
    }
}
