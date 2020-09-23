use std::collections::VecDeque;
use std::mem;

#[derive(Clone)]
pub struct Slab<T> {
    // Chunk of memory
    entries: Vec<Option<T>>,

    // Offset of the next available slot in the slab. Set to the slab's
    // capacity when the slab is full.
    next: VecDeque<usize>,
}

impl<T> Slab<T> {
    /// Construct a new, empty `Slab` with the specified capacity.
    ///
    /// The returned slab will be able to store exactly `capacity` without
    /// reallocating. If `capacity` is 0, the slab will not allocate.
    ///
    /// It is important to note that this function does not specify the *length*
    /// of the returned slab, but only the capacity. For an explanation of the
    /// difference between length and capacity, see [Capacity and
    /// reallocation](index.html#capacity-and-reallocation).
    ///
    /// # Examples
    ///
    /// ```
    /// # use slab::*;
    /// let mut slab = Slab::with_capacity(10);
    ///
    /// // The slab contains no values, even though it has capacity for more
    /// assert_eq!(slab.len(), 0);
    ///
    /// // These are all done without reallocating...
    /// for i in 0..10 {
    ///     slab.insert(i);
    /// }
    ///
    /// // ...but this may make the slab reallocate
    /// slab.insert(11);
    /// ```
    pub fn with_capacity(mut capacity: usize) -> Slab<T> {
        // slots for replicators
        capacity = 10 + capacity;
        let mut entries = Vec::with_capacity(capacity);
        entries.resize_with(capacity, || None::<T>);

        let next: VecDeque<usize> = (10..capacity).collect();
        Slab { entries, next }
    }

    /// Return a reference to the value associated with the given key.
    ///
    /// If the given key is not associated with a value, then `None` is
    /// returned.
    ///
    /// # Examples
    ///
    /// ```
    /// # use slab::*;
    /// let mut slab = Slab::new();
    /// let key = slab.insert("hello");
    ///
    /// assert_eq!(slab.get(key), Some(&"hello"));
    /// assert_eq!(slab.get(123), None);
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # use slab::*;
    /// let mut slab = Slab::new();
    /// let key = slab.insert("hello");
    ///
    /// *slab.get_mut(key).unwrap() = "world";
    ///
    /// assert_eq!(slab[key], "world");
    /// assert_eq!(slab.get_mut(123), None);
    /// ```
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
    ///
    /// # Panics
    ///
    /// Panics if the number of elements in the vector overflows a `usize`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use slab::*;
    /// let mut slab = Slab::new();
    /// let key = slab.insert("hello");
    /// assert_eq!(slab[key], "hello");
    /// ```
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

    /// Remove and return the value associated with the given key.
    ///
    /// The key is then released and may be associated with future stored
    /// values.
    ///
    /// # Panics
    ///
    /// Panics if `key` is not associated with a value.
    ///
    /// # Examples
    ///
    /// ```
    /// # use slab::*;
    /// let mut slab = Slab::new();
    ///
    /// let hello = slab.insert("hello");
    ///
    /// assert_eq!(slab.remove(hello), "hello");
    /// assert!(!slab.contains(hello));
    /// ```
    pub fn remove(&mut self, key: usize) {
        let _ = mem::replace(&mut self.entries[key], None);
        self.next.push_back(key);
    }
}
