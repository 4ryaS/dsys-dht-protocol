use actix::{Actor, StreamHandler, Addr, AsyncContext, ActorContext, Handler};
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use std::time::{Duration, Instant};
use crate::Message;


#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct BroadcastMessage(pub String);

// Define WebSocket Session
pub struct MyWebSocket {
    hb: Instant, // Last heartbeat time
}


impl Actor for MyWebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
    }
}

impl MyWebSocket {
    fn new() -> Self {
        MyWebSocket {
            hb: Instant::now(),
        }
    }

    fn hb(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(Duration::from_secs(5), |act, ctx| {
            if Instant::now().duration_since(act.hb) > Duration::from_secs(10) {
                println!("WebSocket client heartbeat failed, disconnecting!");
                ctx.stop();
                return;
            }
            ctx.ping(b"");
        });
    }
}

// Implement the handler for BroadcastMessage
impl Handler<BroadcastMessage> for MyWebSocket {
    type Result = ();

    fn handle(&mut self, msg: BroadcastMessage, ctx: &mut Self::Context) {
        ctx.text(msg.0); // Send the message text to the WebSocket client
    }
}

// Handle WebSocket messages
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for MyWebSocket {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            Ok(ws::Message::Text(text)) => ctx.text(text),
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            _ => ctx.stop(),
        }
    }
}

// WebSocket route
pub async fn ws_route(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, actix_web::Error> {
    ws::start(MyWebSocket::new(), &req, stream)
}
