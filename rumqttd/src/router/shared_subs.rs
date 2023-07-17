type ClientType = usize;

pub struct SharedGroup {
    // using Vec over HashSet for maintaining order of iter
    clients: Vec<ClientType>,
    // Index into clients, allows us to skip doing iter everytime
    client_cursor: usize,
    // clients: Cycle<IntoIter<String>>,
    pub next_client: ClientType,
    // strategy: Strategy,
}

impl SharedGroup {
    pub fn new(client: ClientType) -> Self {
        SharedGroup {
            clients: vec![client],
            client_cursor: 0,
            next_client: client,
        }
    }
    pub fn add_client(&mut self, client: ClientType) {
        // let mut updated_clients: Vec<String> = self.clients.collect();
        // updated_clients.push(client);
        // self.clients = updated_clients.into_iter().cycle();
        self.clients.push(client)
    }

    pub fn remove_client(&mut self, client: ClientType) {
        // remove client from vec
        // let updated_clients: Vec<String> = self.clients.filter(|&c| c != client).collect();
        // self.clients = updated_clients.into_iter().cycle();

        self.clients.retain(|c| c != &client);
        //Make sure that we are within bounds and that next client is the correct client.
        self.client_cursor = self.client_cursor % self.clients.len();
        self.next_client = self.clients[self.client_cursor];
    }

    pub fn update_next_client(&mut self) {
        // let mut clients = self.clients.iter().cycle();
        // assert_eq!(
        //     clients.find(|c| *c == &self.next_client),
        //     Some(&self.next_client)
        // );
        // self.next_client = dbg!(*clients.next().unwrap());
        self.client_cursor = (self.client_cursor + 1) % self.clients.len();
        self.next_client = self.clients[self.client_cursor];
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
