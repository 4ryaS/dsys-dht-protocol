use actix_web::{web, App, HttpServer, Responder};
use actix::spawn;
use actix::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use sqlx::{PgPool, Error};

#[derive(Serialize, Debug, Clone)]
pub struct Node {
    pub id: i32,
    pub address: String,
    pub port: i32,
    pub predecessor: Option<i32>,
    pub fingers: HashMap<i32, i32>,
    #[serde(skip_serializing, skip_deserializing)]
    pub db_pool: PgPool, // Add PgPool here
}

// impl Default for Node {
//     fn default() -> Self {
//         Node {
//             id: 0,
//             address: String::new(),
//             port: 0,
//             predecessor: None,
//             fingers: HashMap::new(),
//             db_pool: PgPool::new(), // Provide a valid default or remove `Default` where not necessary
//         }
//     }
// }

impl Node {
    pub fn new(id: i32, address: String, port: i32, db_pool: PgPool) -> Self {
        Node {
            id,
            address,
            port,
            predecessor: None,
            fingers: HashMap::new(),
            db_pool,
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
// impl Handler<JoinMessage> for Node {
//     type Result = ();

//     fn handle(&mut self, msg: JoinMessage, ctx: &mut Self::Context) {
//         if self.predecessor.is_none() {
//             // If the node is alone, it points to itself as predecessor and successor
//             self.predecessor = Some(self.id);
//             println!("Node {} joined as the first node in the DHT.", self.id);
//         } else {
//             // The new node will attempt to find its place in the ring
//             println!("Node {} joining DHT, notifying predecessor.", msg.node_id);
//             let successor_id = self.fingers.get(&0).cloned().unwrap_or(self.id);
            
//             // Send a message to the successor node for further integration
//             let successor_addr = ctx.address().clone();
//             successor_addr.do_send(NotifyJoin { new_node_id: msg.node_id });
//         }
//     }
// }

// impl Handler<JoinMessage> for Node {
//     type Result = ();

//     fn handle(&mut self, msg: JoinMessage, ctx: &mut Self::Context) {
//         if self.predecessor.is_none() {
//             // If the node is alone, it points to itself as predecessor and successor
//             self.predecessor = Some(self.id);
//             println!("Node {} joined as the first node in the DHT.", self.id);
//         } else {
//             // Try to notify the predecessor node
//             println!("Node {} attempting to join the DHT.", msg.node_id);
//             let successor_id = self.fingers.get(&0).cloned().unwrap_or(self.id);

//             // Send a message to successor and handle possible errors
//             let result = ctx.address().try_send(NotifyJoin { new_node_id: msg.node_id });
//             match result {
//                 Ok(_) => println!("Node {} notified its successor successfully.", self.id),
//                 Err(e) => println!("Node {} failed to notify successor: {:?}", self.id, e),
//             }
//         }
//     }
// }
impl Handler<JoinMessage> for Node {
    type Result = ();

    fn handle(&mut self, msg: JoinMessage, _: &mut Self::Context) {
        // Use self.db_pool to access the database
        async move {
            match insert_node(&self.db_pool, self).await {
                Ok(_) => println!("Node {} joined the DHT and stored in database.", self.id),
                Err(e) => println!("Node {} failed to store in database: {:?}", self.id, e),
            }
        };
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
// impl Handler<StabilizeMessage> for Node {
//     type Result = ();

//     fn handle(&mut self, _: StabilizeMessage, ctx: &mut Self::Context) {
//         if let Some(predecessor_id) = self.predecessor {
//             println!("Node {} stabilizing. Current predecessor: {}", self.id, predecessor_id);
//             if self.id == predecessor_id {
//                 // This node has no predecessor; point to itself
//                 self.predecessor = Some(self.id);
//             } else {
//                 // Request predecessor to verify or update the successor
//                 let predecessor_addr = ctx.address().clone();
//                 predecessor_addr.do_send(UpdateSuccessor { new_successor_id: self.id });
//             }
//         }
//     }
// }
impl Handler<StabilizeMessage> for Node {
    type Result = ();

    fn handle(&mut self, _: StabilizeMessage, ctx: &mut Self::Context) {
        if let Some(predecessor_id) = self.predecessor {
            println!("Node {} stabilizing with predecessor {}", self.id, predecessor_id);
            if self.id == predecessor_id {
                // This node has no predecessor; point to itself
                self.predecessor = Some(self.id);
            } else {
                // Attempt to update the successor
                let result = ctx.address().try_send(UpdateSuccessor { new_successor_id: self.id });
                match result {
                    Ok(_) => println!("Node {} updated its successor successfully.", self.id),
                    Err(e) => println!("Node {} failed to update successor: {:?}", self.id, e),
                }
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


pub async fn insert_node(pool: &PgPool, node: &Node) -> Result<(), Error> {
    sqlx::query!(
        "INSERT INTO nodes (id, address, port, predecessor) VALUES ($1, $2, $3, $4)",
        node.id as i32,
        node.address,
        node.port as i32,
        node.predecessor as Option<i32>
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_predecessor(pool: &PgPool, node_id: i32, predecessor: i32) -> Result<(), Error> {
    sqlx::query!(
        "UPDATE nodes SET predecessor = $1 WHERE id = $2",
        predecessor as i32,
        node_id as i32
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_key_value(pool: &PgPool, key: i32, value: String, node_id: i32) -> Result<(), Error> {
    sqlx::query!(
        "INSERT INTO key_values (key, value, node_id) VALUES ($1, $2, $3)",
        key as i32,
        value,
        node_id as i32
    )
    .execute(pool)
    .await?;
    Ok(())
}