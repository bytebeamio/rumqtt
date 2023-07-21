pub struct SharedGroup {
    // using Vec over HashSet for maintaining order of iter
    clients: Vec<String>,
    // Index into clients, allows us to skip doing iter everytime
    client_cursor: usize,
    // TODO; random, sticky strategy
    // we can either have that logic in update_next_client or current_client
    // strategy: Strategy,
}

impl SharedGroup {
    pub fn new() -> Self {
        SharedGroup {
            clients: vec![],
            client_cursor: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub fn current_client(&self) -> Option<&String> {
        self.clients.get(self.client_cursor)
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
            self.client_cursor %= self.clients.len();
        }
    }

    pub fn update_next_client(&mut self) {
        self.client_cursor = (self.client_cursor + 1) % self.clients.len();
    }
}

#[cfg(test)]
mod tests {
    use super::SharedGroup;

    #[test]
    fn performs_round_robin() {
        let mut group = SharedGroup {
            clients: vec![1, 2, 3],
            client_cursor: 0,
            next_client: 1,
        };
        group.update_next_client();
        assert_eq!(group.next_client, 2);
        assert_eq!(group.client_cursor, 1);
        group.update_next_client();
        assert_eq!(group.next_client, 3);
        assert_eq!(group.client_cursor, 2);
        group.update_next_client();
        assert_eq!(group.next_client, 1);
        assert_eq!(group.client_cursor, 0);
        group.add_client(4);
        assert_eq!(group.next_client, 1);
        assert_eq!(group.client_cursor, 0);
    }

    #[test]
    fn handles_round_robin_when_start_removed() {
        // [ A, B, C ] => 0
        // we remove A
        // [ B, C ] => Should be the next client (B)
        let mut group = SharedGroup {
            clients: vec![1, 2, 3],
            client_cursor: 0,
            next_client: 1,
        };
        group.remove_client(1);
        assert_eq!(group.next_client, 2);
        assert_eq!(group.client_cursor, 0);
        group.update_next_client();
        assert_eq!(group.next_client, 3);
        assert_eq!(group.client_cursor, 1);
        group.update_next_client();
        assert_eq!(group.next_client, 2);
        assert_eq!(group.client_cursor, 0);
    }

    #[test]
    fn handles_round_robin_when_last_removed() {
        // [ A, B, C ] => 2 (C)
        // we remove C
        // [ A, B ] => Should be the next client (A)
        let mut group = SharedGroup {
            clients: vec![1, 2, 3],
            client_cursor: 0,
            next_client: 1,
        };
        group.update_next_client();
        assert_eq!(group.next_client, 2);
        assert_eq!(group.client_cursor, 1);
        group.update_next_client();
        assert_eq!(group.next_client, 3);
        assert_eq!(group.client_cursor, 2);
        group.remove_client(3);
        assert_eq!(group.next_client, 1);
        assert_eq!(group.client_cursor, 0);
    }
}
