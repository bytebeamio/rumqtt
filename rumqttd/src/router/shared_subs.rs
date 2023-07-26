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
                // how shall we randomly choose client
                // we might need to add extra dependency
                todo!()
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
    use super::SharedGroup;

    #[test]
    fn performs_round_robin() {
        let mut group = SharedGroup {
            clients: vec![1, 2, 3],
            current_client_index: 0,
            next_client: 1,
        };
        group.update_next_client();
        assert_eq!(group.next_client, 2);
        assert_eq!(group.current_client_index, 1);
        group.update_next_client();
        assert_eq!(group.next_client, 3);
        assert_eq!(group.current_client_index, 2);
        group.update_next_client();
        assert_eq!(group.next_client, 1);
        assert_eq!(group.current_client_index, 0);
        group.add_client(4);
        assert_eq!(group.next_client, 1);
        assert_eq!(group.current_client_index, 0);
    }

    #[test]
    fn handles_round_robin_when_start_removed() {
        // [ A, B, C ] => 0
        // we remove A
        // [ B, C ] => Should be the next client (B)
        let mut group = SharedGroup {
            clients: vec![1, 2, 3],
            current_client_index: 0,
            next_client: 1,
        };
        group.remove_client(1);
        assert_eq!(group.next_client, 2);
        assert_eq!(group.current_client_index, 0);
        group.update_next_client();
        assert_eq!(group.next_client, 3);
        assert_eq!(group.current_client_index, 1);
        group.update_next_client();
        assert_eq!(group.next_client, 2);
        assert_eq!(group.current_client_index, 0);
    }

    #[test]
    fn handles_round_robin_when_last_removed() {
        // [ A, B, C ] => 2 (C)
        // we remove C
        // [ A, B ] => Should be the next client (A)
        let mut group = SharedGroup {
            clients: vec![1, 2, 3],
            current_client_index: 0,
            next_client: 1,
        };
        group.update_next_client();
        assert_eq!(group.next_client, 2);
        assert_eq!(group.current_client_index, 1);
        group.update_next_client();
        assert_eq!(group.next_client, 3);
        assert_eq!(group.current_client_index, 2);
        group.remove_client(3);
        assert_eq!(group.next_client, 1);
        assert_eq!(group.current_client_index, 0);
    }
}
