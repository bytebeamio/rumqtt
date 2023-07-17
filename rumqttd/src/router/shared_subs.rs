type ClientType = usize;

pub struct SharedGroup {
    // using Vec over HashSet for maintaining order of iter
    clients: Vec<ClientType>,
    // clients: Cycle<IntoIter<String>>,
    pub next_client: ClientType,
    // strategy: Strategy,
}

impl SharedGroup {
    pub fn new(client: ClientType) -> Self {
        SharedGroup {
            clients: vec![client],
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
        self.clients.retain(|c| c != &client)
    }

    pub fn update_next_client(&mut self) {
        let mut clients = self.clients.iter().cycle();
        assert_eq!(
            clients.find(|c| *c == &self.next_client),
            Some(&self.next_client)
        );
        self.next_client = dbg!(*clients.next().unwrap());
    }
}
