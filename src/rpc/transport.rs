use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use async_trait::async_trait;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::rpc::protocol::RaftMessage;

/// Maximum accepted serialized Raft message size (16 MiB).
/// Reject oversized frames before allocating their payload buffer.
const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("Node {0} is currently unreachable or partitioned")]
    Unreachable(u64),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Channel closed")]
    Closed,
    #[error("Frame length {0} exceeds the maximum allowed size of {MAX_FRAME_SIZE} bytes")]
    FrameTooLarge(usize),
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, msg: RaftMessage) -> Result<(), TransportError>;
}

/// In-memory simulated network transport supporting dynamic partitions and packet drops.
#[derive(Clone)]
pub struct ChannelTransport {
    pub channels: Arc<RwLock<HashMap<u64, mpsc::UnboundedSender<RaftMessage>>>>,
    /// Partitions: if (A, B) is in isolated_pairs, communication between A and B is blocked.
    pub isolated_pairs: Arc<RwLock<HashMap<(u64, u64), bool>>>,
}

impl Default for ChannelTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl ChannelTransport {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            isolated_pairs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Registers a node's incoming receiver channel.
    pub fn register(&self, node_id: u64, sender: mpsc::UnboundedSender<RaftMessage>) {
        let mut map = self.channels.write().unwrap();
        map.insert(node_id, sender);
    }

    /// Partitions network: isolates node A from node B (bidirectionally).
    pub fn isolate_pair(&self, a: u64, b: u64) {
        let mut pairs = self.isolated_pairs.write().unwrap();
        pairs.insert((a, b), true);
        pairs.insert((b, a), true);
    }

    /// Heals network partition between node A and node B.
    pub fn heal_pair(&self, a: u64, b: u64) {
        let mut pairs = self.isolated_pairs.write().unwrap();
        pairs.remove(&(a, b));
        pairs.remove(&(b, a));
    }

    /// Heals all existing network partitions across the entire cluster.
    pub fn heal_all(&self) {
        let mut pairs = self.isolated_pairs.write().unwrap();
        pairs.clear();
    }

    /// Checks if a communication path is currently partitioned.
    pub fn is_blocked(&self, from: u64, to: u64) -> bool {
        let pairs = self.isolated_pairs.read().unwrap();
        pairs.get(&(from, to)).copied().unwrap_or(false)
    }
}

#[async_trait]
impl Transport for ChannelTransport {
    async fn send(&self, msg: RaftMessage) -> Result<(), TransportError> {
        if self.is_blocked(msg.from, msg.to) {
            return Err(TransportError::Unreachable(msg.to));
        }

        let sender = {
            let map = self.channels.read().unwrap();
            map.get(&msg.to).cloned()
        };

        if let Some(tx) = sender {
            tx.send(msg).map_err(|_| TransportError::Closed)?;
            Ok(())
        } else {
            Err(TransportError::Unreachable(msg.to))
        }
    }
}

/// TCP Framing utility for wire encoding / decoding.
pub struct TcpCodec;

impl TcpCodec {
    /// Writes a framed RaftMessage to an async TCP stream.
    pub async fn write_message<W: AsyncWriteExt + Unpin>(
        writer: &mut W,
        msg: &RaftMessage,
    ) -> Result<(), TransportError> {
        let bytes = serde_json::to_vec(msg)?;
        let len = (bytes.len() as u32).to_be_bytes();
        writer.write_all(&len).await?;
        writer.write_all(&bytes).await?;
        writer.flush().await?;
        Ok(())
    }

    /// Reads a framed RaftMessage from an async TCP stream.
    pub async fn read_message<R: AsyncReadExt + Unpin>(
        reader: &mut R,
    ) -> Result<RaftMessage, TransportError> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_FRAME_SIZE {
            return Err(TransportError::FrameTooLarge(len));
        }

        let mut data = vec![0u8; len];
        reader.read_exact(&mut data).await?;
        let msg = serde_json::from_slice(&data)?;
        Ok(msg)
    }
}


#[cfg(test)]
mod tcp_codec_tests {
    use super::*;
    use tokio::io::{duplex, AsyncWriteExt};

    #[tokio::test]
    async fn rejects_oversized_frame_before_reading_payload() {
        let (mut writer, mut reader) = duplex(8);
        writer.write_all(&((MAX_FRAME_SIZE as u32) + 1).to_be_bytes()).await.unwrap();

        let result = TcpCodec::read_message(&mut reader).await;
        assert!(matches!(result, Err(TransportError::FrameTooLarge(size)) if size == MAX_FRAME_SIZE + 1));
    }

    #[tokio::test]
    async fn accepts_frame_at_maximum_size() {
        let (mut writer, mut reader) = duplex(MAX_FRAME_SIZE + 4);
        let payload = serde_json::to_vec(&RaftMessage {
            group_id: 1,
            from: 1,
            to: 2,
            payload: crate::rpc::protocol::RaftPayload::RequestVote(
                crate::rpc::protocol::RequestVoteArgs {
                    term: 1,
                    candidate_id: 1,
                    last_log_index: 0,
                    last_log_term: 0,
                },
            ),
        }).unwrap();
        assert!(payload.len() <= MAX_FRAME_SIZE);
        writer.write_all(&(payload.len() as u32).to_be_bytes()).await.unwrap();
        writer.write_all(&payload).await.unwrap();

        let decoded = TcpCodec::read_message(&mut reader).await.unwrap();
        assert_eq!(decoded.group_id, 1);
    }
}
