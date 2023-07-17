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
        self.update_next_client()
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
