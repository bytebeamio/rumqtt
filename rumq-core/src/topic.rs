use std::iter::Iterator;

// checks if a topic or topic filter has wildcards
pub fn has_wildcards(s: &str) -> bool {
    s.contains("+") || s.contains("#")
}

/// checks if a topic is valid
pub fn valid_topic(topic: &str) -> bool {
    if topic.contains("+") {
        return false;
    }

    if topic.contains("#") {
        return false;
    }

    true
}

/// checks if the filter is valid
/// https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106
pub fn valid_filter(filter: &str) -> bool {
    let hirerarchy = filter.split("/").collect::<Vec<&str>>();

    if let Some((last, remaining)) = hirerarchy.split_last() {
        // # is not allowed in filer except as a last entry
        // invalid: sport/tennis#/player
        // invalid: sport/tennis/#/ranking
        for entry in remaining.iter() {
            if entry.contains("#") {
                return false;
            }
        }

        // only single '#" is allowed in last entry
        // invalid: sport/tennis#
        if last.len() != 1 && last.contains("#") {
            return false;
        }
    }

    true
}

/// checks if topic matches a filter. topic and filter validation isn't done here.
/// NOTE: make sure a topic is validated during a publish and filter is validated
/// during a subscribe
pub fn matches(topic: &str, filter: &str) -> bool {
    let mut topics = topic.split("/");
    let mut filters = filter.split("/");

    for f in filters.by_ref() {
        // "#" being the last element is validated by the broker with 'valid_filter'
        if f == "#" {
            return true;
        }

        // filter still has remaining elements
        // filter = a/b/c/# should match topci = a/b/c
        // filter = a/b/c/d should not match topic = a/b/c
        let top = topics.next();
        match top {
            Some(_) if f == "+" => continue,
            Some(t) if f != t => return false,
            Some(_) => continue,
            None => return false,
        }
    }

    // topic has remaining elements and filter's last element isn't "#"
    if topics.next().is_some() {
        return false;
    }

    true
}
#[cfg(test)]
mod test {
    #[test]
    fn wildcards_are_detected_correctly() {
        assert!(!super::has_wildcards("a/b/c"));
        assert!(super::has_wildcards("a/+/c"));
        assert!(super::has_wildcards("a/b/#"));
    }

    #[test]
    fn topics_are_validated_correctly() {
        assert!(!super::valid_topic("+wrong"));
        assert!(!super::valid_topic("wro#ng"));
        assert!(!super::valid_topic("w/r/o/n/g+"));
        assert!(!super::valid_topic("wrong/#/path"));
    }

    #[test]
    fn filters_are_validated_correctly() {
        assert!(!super::valid_filter("wrong/#/filter"));
        assert!(!super::valid_filter("wrong/wr#ng/filter"));
        assert!(!super::valid_filter("wrong/filter#"));
        assert!(super::valid_filter("correct/filter/#"));
    }

    #[test]
    fn topics_match_with_filters_as_expected() {
        let topic = "a/b/c";
        let filter = "a/b/c";
        assert!(super::matches(topic, filter));

        let topic = "a/b/c";
        let filter = "d/b/c";
        assert!(!super::matches(topic, filter));

        let topic = "a/b/c";
        let filter = "a/b/e";
        assert!(!super::matches(topic, filter));

        let topic = "a/b/c";
        let filter = "a/b/c/d";
        assert!(!super::matches(topic, filter));

        let topic = "a/b/c";
        let filter = "a/b/c/#";
        assert!(super::matches(topic, filter));

        let topic = "a/b/c/d";
        let filter = "a/b/c";
        assert!(!super::matches(topic, filter));

        let topic = "a/b/c/d";
        let filter = "a/b/c/#";
        assert!(super::matches(topic, filter));

        let topic = "a/b/c/d/e/f";
        let filter = "a/b/c/#";
        assert!(super::matches(topic, filter));

        let topic = "a/b/c";
        let filter = "a/+/c";
        assert!(super::matches(topic, filter));
        let topic = "a/b/c/d/e";
        let filter = "a/+/c/+/e";
        assert!(super::matches(topic, filter));

        let topic = "a/b";
        let filter = "a/b/+";
        assert!(!super::matches(topic, filter));
    }
}
