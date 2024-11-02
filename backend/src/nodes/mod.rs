// src/nodes/mod.rs

// Declare the module within nodes
pub mod node_actor;

// Re-export structs for easy access
pub use node_actor::{Node, JoinMessage, StabilizeMessage, FixFingersMessage, LookupMessage, HealthCheck, FingerTableUpdate};
