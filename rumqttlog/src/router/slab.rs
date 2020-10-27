use std::collections::VecDeque;
use std::mem;

#[derive(Clone, Debug)]
pub struct Slab<T> {
    // Chunk of memory
    entries: Vec<Option<T>>,

    // Offset of the next available slot in the slab. Set to the slab's
    // capacity when the slab is full.
    next: VecDeque<usize>,
}

impl<T> Slab<T> {
    /// Constructs a new initialized slab
    pub fn with_capacity(mut capacity: usize) -> Slab<T> {
        // slots for replicators
        capacity += 10;
        let mut entries = Vec::with_capacity(capacity);
        entries.resize_with(capacity, || None::<T>);

        let next: VecDeque<usize> = (10..capacity).collect();
        Slab { entries, next }
    }

    /// Return a reference to the value associated with the given key.
    ///
    /// If the given key is not associated with a value, then `None` is
    /// returned.
    pub fn _get(&self, key: usize) -> Option<&T> {
        match self.entries.get(key) {
            Some(v) => v.as_ref(),
            None => None,
        }
    }

    /// Return a mutable reference to the value associated with the given key.
    ///
    /// If the given key is not associated with a value, then `None` is
    /// returned.
    pub fn get_mut(&mut self, key: usize) -> Option<&mut T> {
        match self.entries.get_mut(key) {
            Some(v) => v.as_mut(),
            None => None,
        }
    }

    /// Insert a value in the slab, returning key assigned to the value.
    ///
    /// The returned key can later be used to retrieve or remove the value using indexed
    /// lookup and `remove`. Additional capacity is allocated if needed. See
    /// [Capacity and reallocation](index.html#capacity-and-reallocation).
    pub fn insert(&mut self, val: T) -> Option<usize> {
        let key = match self.next.pop_front() {
            Some(key) => key,
            None => return None,
        };

        if let Some(Some(_)) = self.entries.get(key) {
            panic!("Inserting in a non vacant space")
        };

        self.entries[key] = Some(val);
        Some(key)
    }

    pub fn insert_at(&mut self, val: T, at: usize) {
        if let Some(Some(_)) = self.entries.get(at) {
            panic!("Inserting in a non vacant space")
        };

        self.entries[at] = Some(val);
    }

    pub fn remove(&mut self, key: usize) -> Option<T> {
        let data = mem::replace(&mut self.entries[key], None);
        self.next.push_back(key);
        data
    }
}
