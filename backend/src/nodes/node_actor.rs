use actix_web::{web, App, HttpServer, Responder};
use actix::spawn;
use actix::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use sqlx::{PgPool, Error, query};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

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

#[derive(Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: i64,
    pub address: String,
    pub port: i64,
    pub predecessor: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KeyValue {
    pub key: i32,
    pub value: String,
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
        // Insert the node into the nodes table
        let pool = db_pool.clone();
        let address_clone = address.clone();
        actix::spawn(async move {
            query!(
                "INSERT INTO nodes (id, address, port) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                id as i32,
                address_clone,
                port as i32
            )
            .execute(&pool)
            .await
            .expect("Failed to insert node into nodes table");
        });

        Node {
            id,
            address,
            port,
            predecessor: None,
            fingers: HashMap::new(),
            db_pool,
        }
    }

    fn schedule_finger_table_update(&self, ctx: &mut Context<Self>) {
        ctx.run_interval(Duration::from_secs(60), |node, _ctx| {
            node.update_finger_table();
        });
    }

    fn update_finger_table(&mut self) {
        for i in 0..self.fingers.len() as i32 {
            let target_id = (self.id + 2_i32.pow(i as u32)) % 1024; // Example ring size
            if let Some(closest_node) = self.closest_preceding_node(target_id) {
                self.fingers.insert(i, closest_node);
            }
        }
        println!("Node {}: Finger table updated.", self.id);
    }

    fn closest_preceding_node(&self, id: i32) -> Option<i32> {
        // Collect fingers into a Vec and reverse for ordered access
        let fingers_vec: Vec<_> = self.fingers.iter().collect();
        fingers_vec.iter().rev().find_map(|(_, &node_id)| {
            if node_id < id {
                Some(node_id)
            } else {
                None
            }
        })
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
        let pool = self.db_pool.clone();
        let node_id = msg.node_id;
        let address = self.address.clone();
        let port = self.port;

        actix::spawn(async move {
            // Step 1: Find the closest predecessor in the database
            let closest_predecessor = sqlx::query!(
                "SELECT id FROM nodes WHERE id < $1 ORDER BY id DESC LIMIT 1",
                node_id as i32
            )
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten()
            .map(|record| record.id);

            // Step 2: Set the predecessor for the new node
            let predecessor_id = closest_predecessor;
            match sqlx::query!(
                "INSERT INTO nodes (id, address, port, predecessor) VALUES ($1, $2, $3, $4)
                ON CONFLICT (id) DO UPDATE SET predecessor = $4",
                node_id as i32,
                address,
                port as i32,
                predecessor_id
            )
            .execute(&pool)
            .await
            {
                Ok(_) => println!("Node {} joined the DHT with predecessor {:?}", node_id, predecessor_id),
                Err(e) => println!("Failed to insert or update node {} in database: {:?}", node_id, e),
            }

            // Step 3: Optional - Update the successor of the predecessor if needed
            if let Some(pred_id) = predecessor_id {
                let _ = sqlx::query!(
                    "UPDATE nodes SET successor = $1 WHERE id = $2",
                    node_id as i32,
                    pred_id
                )
                .execute(&pool)
                .await;
            }
        });
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
        let key_hash = hash_key(&msg.key.to_string());

        // Check if this node is responsible for the key
        if self.id == (key_hash % 1024) as i32 {
            Some(self.id)
        } else {
            // Route lookup request to the closest preceding node
            if let Some(next_node_id) = self.closest_preceding_node(key_hash as i32) {
                println!("Node {} forwarding lookup for key {} to node {}", self.id, msg.key, next_node_id);
                None // In a complete implementation, you'd forward the request
            } else {
                println!("Node {}: No closer node found for key {}", self.id, msg.key);
                None
            }
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

impl KeyValue {
    pub async fn insert(pool: &PgPool, key: i32, value: String, node_id: i32) -> Result<(), sqlx::Error> {
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

    pub async fn get(pool: &PgPool, key: i32) -> Result<Option<String>, sqlx::Error> {
        let result = sqlx::query!("SELECT value FROM key_values WHERE key = $1", key as i32)
            .fetch_optional(pool)
            .await?;
        Ok(result.map(|r| r.value))
    }

    pub async fn delete(pool: &PgPool, key: i32) -> Result<(), sqlx::Error> {
        sqlx::query!("DELETE FROM key_values WHERE key = $1", key as i32)
            .execute(pool)
            .await?;
        Ok(())
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), sqlx::Error>")]
pub struct InsertKeyValue {
    pub key: i32,
    pub value: String,
}

#[derive(Message)]
#[rtype(result = "Result<Option<String>, sqlx::Error>")]
pub struct GetKeyValue {
    pub key: i32,
}

#[derive(Message)]
#[rtype(result = "Result<(), sqlx::Error>")]
pub struct DeleteKeyValue {
    pub key: i32,
}

impl Handler<InsertKeyValue> for Node {
    type Result = Result<(), sqlx::Error>;

    fn handle(&mut self, msg: InsertKeyValue, ctx: &mut Self::Context) -> Self::Result {
        let pool = self.db_pool.clone();
        let node_id = self.id;

        // Clone msg.value so it can be used multiple times
        let value_for_insert = msg.value.clone();
        let value_for_successor = msg.value.clone();
        let value_for_predecessor = msg.value.clone();

        // Insert into the current node's database
        actix::spawn(async move {
            KeyValue::insert(&pool, msg.key, value_for_insert, node_id).await.unwrap();
        });

        // Replicate to successor and predecessor
        if let Some(successor_id) = self.fingers.get(&0) {
            let successor_addr = ctx.address().clone();
            successor_addr.do_send(ReplicateData {
                key: msg.key,
                value: value_for_successor,
            });
        }

        if let Some(predecessor_id) = self.predecessor {
            let predecessor_addr = ctx.address().clone();
            predecessor_addr.do_send(ReplicateData {
                key: msg.key,
                value: value_for_predecessor,
            });
        }

        Ok(())
    }
}


impl Handler<GetKeyValue> for Node {
    type Result = ResponseFuture<Result<Option<String>, sqlx::Error>>;

    fn handle(&mut self, msg: GetKeyValue, _: &mut Self::Context) -> Self::Result {
        let pool = self.db_pool.clone();
        let key = msg.key;

        Box::pin(async move {
            match KeyValue::get(&pool, key).await {
                Ok(value) => Ok(value),  // Return the Option<String> from KeyValue::get
                Err(e) => Err(e),
            }
        })
    }
}

impl Handler<DeleteKeyValue> for Node {
    type Result = Result<(), sqlx::Error>;

    fn handle(&mut self, msg: DeleteKeyValue, _: &mut Self::Context) -> Self::Result {
        let pool = self.db_pool.clone();
        let key = msg.key;

        actix::spawn(async move {
            KeyValue::delete(&pool, key).await.unwrap();
        });
        Ok(())
    }
}

pub fn hash_key(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct HealthCheck;

#[derive(Message)]
#[rtype(result = "()")]
pub struct Heartbeat;

impl Handler<Heartbeat> for Node {
    type Result = ();

    fn handle(&mut self, _: Heartbeat, _: &mut Self::Context) -> Self::Result {
        // Respond to the heartbeat to indicate this node is alive
        println!("Node {} received heartbeat", self.id);
    }
}

impl Handler<HealthCheck> for Node {
    type Result = ();

    fn handle(&mut self, _: HealthCheck, ctx: &mut Self::Context) {
        let node_id = self.id; // Copy self.id to a local variable

        if let Some(successor_id) = self.fingers.get(&0) {
            let successor_addr = ctx.address().clone();
            actix::spawn(async move {
                match successor_addr.send(Heartbeat).await {
                    Ok(_) => println!("Node {}: Successor is alive", node_id),
                    Err(_) => println!("Node {}: Successor failed health check", node_id),
                }
            });
        }

        if let Some(predecessor_id) = self.predecessor {
            let predecessor_addr = ctx.address().clone();
            actix::spawn(async move {
                match predecessor_addr.send(Heartbeat).await {
                    Ok(_) => println!("Node {}: Predecessor is alive", node_id),
                    Err(_) => println!("Node {}: Predecessor failed health check", node_id),
                }
            });
        }
    }
}


#[derive(Message, Deserialize, Serialize)]
#[rtype(result = "Result<(), sqlx::Error>")]
pub struct ReplicateData {
    pub key: i32,
    pub value: String,
}

impl Handler<ReplicateData> for Node {
    type Result = Result<(), sqlx::Error>;

    fn handle(&mut self, msg: ReplicateData, _: &mut Self::Context) -> Self::Result {
        let pool = self.db_pool.clone();
        let node_id = self.id;

        actix::spawn(async move {
            KeyValue::insert(&pool, msg.key, msg.value, node_id).await.unwrap();
        });
        Ok(())
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), sqlx::Error>")]
pub struct TransferData {
    pub data: Vec<KeyValue>, // List of key-value pairs to transfer
}

impl Handler<TransferData> for Node {
    type Result = Result<(), sqlx::Error>;

    fn handle(&mut self, msg: TransferData, _: &mut Self::Context) -> Self::Result {
        let node_id = self.id;

        for kv in msg.data {
            let pool_clone = self.db_pool.clone(); // Clone pool for each async task

            actix::spawn(async move {
                KeyValue::insert(&pool_clone, kv.key, kv.value, node_id).await.unwrap();
            });
        }

        Ok(())
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct FingerTableUpdate;

impl Handler<FingerTableUpdate> for Node {
    type Result = ();

    fn handle(&mut self, _: FingerTableUpdate, _: &mut Self::Context) -> Self::Result {
        self.update_finger_table();
    }
}