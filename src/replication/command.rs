use super::{simple, synchronization};

#[derive(Clone)]
pub enum Command {
    Simple(simple::Simple),
    Synchronization(synchronization::Synchronization),
}