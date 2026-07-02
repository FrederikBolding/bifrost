use anyhow::{Result, anyhow};
use iroh::{
    Endpoint,
    endpoint::{Connection, RecvStream, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_tickets::{Ticket, endpoint::EndpointTicket};
use std::env;
use std::sync::Arc;
use tokio::{io::AsyncWriteExt, sync::Mutex};
use tokio::{fs::File, io::AsyncReadExt};

const ALPN: &[u8; 10] = b"bifrost/v0";

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let file_or_ticket = args
        .next()
        .ok_or_else(|| anyhow!("[bifrost] Expected at least one argument."))?;

    let ticket = EndpointTicket::decode_string(&file_or_ticket).ok();

    if let Some(ticket) = ticket {
        return receive(ticket).await;
    }

    let file = File::open(file_or_ticket).await?;

    send(file).await
}

async fn send(file: File) -> Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;

    endpoint.online().await;

    let ticket = EndpointTicket::new(endpoint.addr());

    println!("[bifrost] Use the following ticket to connect {}", ticket);

    let bifrost = Bifrost::new(file);

    let router = Router::builder(endpoint).accept(ALPN, bifrost).spawn();

    tokio::signal::ctrl_c().await?;

    Ok(())
}

async fn receive(ticket: EndpointTicket) -> Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;

    let connection = endpoint
        .connect(ticket.endpoint_addr().clone(), ALPN)
        .await?;

    let (mut send, mut recv) = connection.open_bi().await?;

    println!("[bifrost] Connection established!");

    send.write_all(b"START").await?;

    let response = recv.read_to_end(100_000_000).await?;

    println!("[bifrost] Received {} bytes", response.len());

    send.write_all(b"OK").await?;
    send.finish()?;

    connection.closed().await;

    let mut file = File::create("out.bin").await?;

    file.write_all(&response).await?;

    Ok(())
}
#[derive(Debug)]
struct Bifrost {
    file: Arc<Mutex<File>>,
}

impl ProtocolHandler for Bifrost {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        println!("[bifrost] Connection established!");

        let _request = recv_bytes(&mut recv, 5).await.unwrap();

        let mut file = self.file.lock().await;

        // TODO
        // Sender: Hash, compress, send
        // Receiver: Receive, decompress, hash
        let bytes_sent = tokio::io::copy(&mut *file, &mut send).await.unwrap();

        println!("[bifrost] Sent {} bytes", bytes_sent);

        send.finish()?;

        let _response = recv_bytes(&mut recv, 2).await.unwrap();

        connection.close(0u32.into(), b"OK");

        Ok(())
    }
}

impl Bifrost {
    pub fn new(file: File) -> Self {
        Self {
            file: Arc::new(Mutex::new(file)),
        }
    }
}

async fn recv_bytes(stream: &mut RecvStream, length: usize) -> Result<Vec<u8>> {
    let mut buffer = vec![0u8; length];
    stream.read_exact(&mut buffer).await?;
    Ok(buffer)
}
