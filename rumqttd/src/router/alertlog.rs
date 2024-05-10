use crate::RouterConfig;
use std::collections::VecDeque;

pub mod alert {
    use serde::Serialize;

    #[derive(Serialize, Debug, Clone)]
    pub enum AlertKind {
        CursorJump { filter: String, lost: usize },
        BadPublish { topic: String },
    }

    impl AlertKind {
        pub fn name(&self) -> String {
            match self {
                Self::CursorJump { .. } => "cursor_jump".to_owned(),
                Self::BadPublish { .. } => "bad_publish".to_owned(),
            }
        }

        pub fn description(&self) -> String {
            match self {
                Self::CursorJump { filter, lost, .. } => format!("Filter: {filter}, Lost: {lost}"),
                Self::BadPublish { topic, .. } => format!("Topic: {topic}"),
            }
        }
    }

    #[derive(Serialize, Debug, Clone)]
    pub struct Alert {
        pub timestamp: u128,
        pub sequence: usize,
        pub client_id: String,
        pub kind: AlertKind,
    }

    pub fn cursorjump(client_id: &str, filter: &str, lost: usize) -> Alert {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        Alert {
            timestamp,
            sequence: 0,
            client_id: client_id.to_owned(),
            kind: AlertKind::CursorJump {
                filter: filter.to_owned(),
                lost,
            },
        }
    }

    pub fn _badpublish(client_id: &str, topic: &str) -> Alert {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        Alert {
            timestamp,
            sequence: 0,
            client_id: client_id.to_owned(),
            kind: AlertKind::BadPublish {
                topic: topic.to_owned(),
            },
        }
    }
}

pub use alert::*;

pub struct AlertLog {
    pub _config: RouterConfig,
    pub alerts: VecDeque<Alert>,
}

impl AlertLog {
    pub fn new(_config: RouterConfig) -> AlertLog {
        AlertLog {
            _config,
            alerts: VecDeque::with_capacity(100),
        }
    }

    pub fn log(&mut self, mut alert: Alert) {
        alert.sequence = match self.alerts.back() {
            Some(alert) => alert.sequence + 1,
            None => 0,
        };

        self.alerts.push_back(alert);
        if self.alerts.len() >= 100 {
            self.alerts.pop_front();
        }
    }

    pub fn take(&mut self) -> VecDeque<Alert> {
        self.alerts.split_off(0)
    }
}
