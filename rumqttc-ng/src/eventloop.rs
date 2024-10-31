use std::{thread, time::Duration};


pub struct EventLoopSettings {
    pub max_subscriptions: usize,
    pub max_subscription_log_size: usize,
    pub max_inflight_messages: usize,
}

pub struct EventLoop {
    settings: EventLoopSettings,
}

impl EventLoop {
    pub fn new(settings: EventLoopSettings) -> Self {
        EventLoop { settings }
    }

    pub fn start(&self) {
        loop {
            thread::sleep(Duration::from_millis(100));
            println!("EventLoop::start");
        }
    }
}

impl Default for EventLoopSettings {
    fn default() -> Self {
        EventLoopSettings {
            max_subscriptions: 10,
            max_subscription_log_size: 100 * 1024 * 1024,
            max_inflight_messages: 100,
        }
    }
}