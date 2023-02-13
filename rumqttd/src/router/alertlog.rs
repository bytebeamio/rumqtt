use crate::RouterConfig;
use std::collections::VecDeque;

pub mod alert {
    #[derive(Debug, Clone)]
    pub enum Alert {
        Warn(Warn),
        Error(Error),
    }

    #[derive(Debug, Clone)]
    pub enum Warn {
        CursorJump { filter: String, lost: usize },
    }

    #[derive(Debug, Clone)]
    pub enum Error {
        BadPublish { client_id: String, topic: String },
    }

    pub fn cursorjump(filter: &str, lost: usize) -> Alert {
        Alert::Warn(Warn::CursorJump {
            filter: filter.to_owned(),
            lost,
        })
    }

    pub fn badpublish(client_id: &str, topic: &str) -> Alert {
        Alert::Error(Error::BadPublish {
            client_id: client_id.to_owned(),
            topic: topic.to_owned(),
        })
    }
}

pub use alert::*;

pub struct AlertLog {
    pub config: RouterConfig,
    pub alerts: VecDeque<Alert>,
}

impl AlertLog {
    pub fn new(config: RouterConfig) -> AlertLog {
        AlertLog {
            config,
            alerts: VecDeque::with_capacity(100),
        }
    }

    pub fn log(&mut self, alert: Alert) {
        self.alerts.push_back(alert);
        if self.alerts.len() >= 100 {
            self.alerts.pop_front();
        }
    }

    pub fn take(&mut self) -> VecDeque<Alert> {
        self.alerts.split_off(0)
    }
}
