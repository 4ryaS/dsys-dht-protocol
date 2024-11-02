use actix::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub id: i32,
    pub address: String,
    pub port: i32,
    pub predecessor: Option<i32>,
    pub fingers: HashMap<i32, i32>, // Maps finger index to node ID
}

impl Node {
    pub fn new(id: i32, address: String, port: i32) -> Self {
        Node {
            id,
            address,
            port,
            predecessor: None,
            fingers: HashMap::new(),
        }
    }

    // Helper function to find the closest preceding node in the finger table
    fn closest_preceding_node(&self, key: i32) -> i32 {
        for (_, &node_id) in self.fingers.iter().collect::<Vec<_>>().iter().rev() {
            if node_id < key {
                return node_id;
            }
        }
        self.id
    }
}

// Messages for various DHT functions
#[derive(Message)]
#[rtype(result = "()")]
pub struct JoinMessage {
    pub node_id: i32,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct StabilizeMessage;

#[derive(Message)]
#[rtype(result = "()")]
pub struct FixFingersMessage;

#[derive(Message)]
#[rtype(result = "Option<i32>")]
pub struct LookupMessage {
    pub key: i32,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct NotifyJoin {
    pub new_node_id: i32,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct UpdateSuccessor {
    pub new_successor_id: i32,
}

// Implementing the Actor trait for Node
impl Actor for Node {
    type Context = Context<Self>;
}

// Handler for JoinMessage
impl Handler<JoinMessage> for Node {
    type Result = ();

    fn handle(&mut self, msg: JoinMessage, ctx: &mut Self::Context) {
        if self.predecessor.is_none() {
            // If the node is alone, it points to itself as predecessor and successor
            self.predecessor = Some(self.id);
            println!("Node {} joined as the first node in the DHT.", self.id);
        } else {
            // The new node will attempt to find its place in the ring
            println!("Node {} joining DHT, notifying predecessor.", msg.node_id);
            let successor_id = self.fingers.get(&0).cloned().unwrap_or(self.id);
            
            // Send a message to the successor node for further integration
            let successor_addr = ctx.address().clone();
            successor_addr.do_send(NotifyJoin { new_node_id: msg.node_id });
        }
    }
}

// Handler for NotifyJoin message
impl Handler<NotifyJoin> for Node {
    type Result = ();

    fn handle(&mut self, msg: NotifyJoin, _: &mut Self::Context) {
        println!("Node {} notified of new join for node {}", self.id, msg.new_node_id);
        // Update successor or other relevant attributes as necessary
    }
}

// Handler for StabilizeMessage
impl Handler<StabilizeMessage> for Node {
    type Result = ();

    fn handle(&mut self, _: StabilizeMessage, ctx: &mut Self::Context) {
        if let Some(predecessor_id) = self.predecessor {
            println!("Node {} stabilizing. Current predecessor: {}", self.id, predecessor_id);
            if self.id == predecessor_id {
                // This node has no predecessor; point to itself
                self.predecessor = Some(self.id);
            } else {
                // Request predecessor to verify or update the successor
                let predecessor_addr = ctx.address().clone();
                predecessor_addr.do_send(UpdateSuccessor { new_successor_id: self.id });
            }
        }
    }
}

// Handler for UpdateSuccessor message
impl Handler<UpdateSuccessor> for Node {
    type Result = ();

    fn handle(&mut self, msg: UpdateSuccessor, _: &mut Self::Context) {
        println!("Node {} updating successor to node {}", self.id, msg.new_successor_id);
        // Update successor information if the new successor is more appropriate
    }
}

// Handler for FixFingersMessage
impl Handler<FixFingersMessage> for Node {
    type Result = ();

    fn handle(&mut self, _: FixFingersMessage, _: &mut Self::Context) {
        for i in 0..self.fingers.len() as i32 {
            // Simple simulation of finger table update
            self.fingers.insert(i, (self.id + 2_i32.pow(i as u32)) % 1024);
            println!("Node {} fixing finger {}", self.id, i);
        }
    }
}

// Handler for LookupMessage
impl Handler<LookupMessage> for Node {
    type Result = Option<i32>;

    fn handle(&mut self, msg: LookupMessage, _: &mut Self::Context) -> Self::Result {
        if msg.key == self.id {
            println!("Node {} is responsible for key {}", self.id, msg.key);
            Some(self.id)
        } else {
            // Route lookup request to the closest preceding node in finger table
            let next_node_id = self.closest_preceding_node(msg.key);
            println!("Node {} forwarding lookup for key {} to node {}", self.id, msg.key, next_node_id);
            None // In a real scenario, this would pass the request to `next_node_id`
        }
    }
} 
