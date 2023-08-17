use rand::Rng;
use serde::{Deserialize, Serialize};

pub struct SharedGroup {
    // using Vec over HashSet for maintaining order of iter
    clients: Vec<String>,
    // Index into clients, allows us to skip doing iter everytime
    current_client_index: usize,
    pub cursor: (u64, u64),
    pub strategy: Strategy,
}

impl SharedGroup {
    pub fn new(cursor: (u64, u64), strategy: Strategy) -> Self {
        SharedGroup {
            clients: vec![],
            current_client_index: 0,
            cursor,
            strategy,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub fn current_client(&self) -> Option<&String> {
        self.clients.get(self.current_client_index)
    }

    pub fn add_client(&mut self, client: String) {
        self.clients.push(client)
    }

    pub fn remove_client(&mut self, client: &String) {
        // remove client from vec
        self.clients.retain(|c| c != client);

        // if there are no clients left, we have to avoid % by 0
        if !self.clients.is_empty() {
            // Make sure that we are within bounds and that next client is the correct client.
            self.current_client_index %= self.clients.len();
        }
    }

    pub fn update_next_client(&mut self) {
        match self.strategy {
            Strategy::RoundRobin => {
                self.current_client_index = (self.current_client_index + 1) % self.clients.len();
            }
            Strategy::Random => {
                self.current_client_index = rand::thread_rng().gen_range(0..self.clients.len());
            }
            Strategy::Sticky => {}
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Strategy {
    #[default]
    RoundRobin,
    Random,
    Sticky,
}

#[cfg(test)]
mod tests {
    use crate::router::shared_subs::Strategy;

    use super::SharedGroup;

    #[test]
    fn performs_round_robin() {
        let mut group = SharedGroup {
            clients: vec!["A".into(), "B".into(), "C".into()],
            current_client_index: 0,
            cursor: (0, 0),
            strategy: Strategy::RoundRobin,
        };
        group.update_next_client();
        assert_eq!(group.current_client_index, 1);
        group.update_next_client();
        assert_eq!(group.current_client_index, 2);
        group.update_next_client();
        assert_eq!(group.current_client_index, 0);
        group.add_client("D".into());
        assert_eq!(group.current_client_index, 0);
    }

    #[test]
    fn handles_round_robin_when_start_removed() {
        // [ A, B, C ] => 0
        // we remove A
        // [ B, C ] => Should be the next client (B)
        let mut group = SharedGroup {
            clients: vec!["A".into(), "B".into(), "C".into()],
            current_client_index: 0,
            cursor: (0, 0),
            strategy: Strategy::RoundRobin,
        };
        group.remove_client(&"A".into());
        assert_eq!(group.current_client_index, 0);
        group.update_next_client();
        assert_eq!(group.current_client_index, 1);
        group.update_next_client();
        assert_eq!(group.current_client_index, 0);
    }

    #[test]
    fn handles_round_robin_when_last_removed() {
        // [ A, B, C ] => 2 (C)
        // we remove C
        // [ A, B ] => Should be the next client (A)
        let mut group = SharedGroup {
            clients: vec!["A".into(), "B".into(), "C".into()],
            current_client_index: 0,
            cursor: (0, 0),
            strategy: Strategy::RoundRobin,
        };
        group.update_next_client();
        assert_eq!(group.current_client_index, 1);
        group.update_next_client();
        assert_eq!(group.current_client_index, 2);
        group.remove_client(&"C".into());
        assert_eq!(group.current_client_index, 0);
    }
}
