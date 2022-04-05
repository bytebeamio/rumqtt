/// Checks if a topic or topic filter has wildcards
pub fn has_wildcards(s: &str) -> bool {
    s.contains('+') || s.contains('#')
}

/// Checks if topic matches a filter. topic and filter validation isn't done here.
///
/// **NOTE**: 'topic' is a misnomer in the arg. this can also be used to match 2 wild subscriptions
/// **NOTE**: make sure a topic is validated during a publish and filter is validated
/// during a subscribe
pub fn matches(topic: &str, filter: &str) -> bool {
    if !topic.is_empty() && topic[..1].contains('$') {
        return false;
    }

    let mut topics = topic.split('/');
    let mut filters = filter.split('/');

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
            Some(t) if t == "#" => return false,
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
    fn dollar_subscriptions_doesnt_match_dollar_topic() {
        assert!(super::matches("sy$tem/metrics", "sy$tem/+"));
        assert!(!super::matches("$system/metrics", "$system/+"));
        assert!(!super::matches("$system/metrics", "+/+"));
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
        let filter = "#";
        assert!(super::matches(topic, filter));

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

        let filter1 = "a/b/+";
        let filter2 = "a/b/#";
        assert!(super::matches(filter1, filter2));
        assert!(!super::matches(filter2, filter1));

        let filter1 = "a/b/+";
        let filter2 = "#";
        assert!(super::matches(filter1, filter2));

        let filter1 = "a/+/c/d";
        let filter2 = "a/+/+/d";
        assert!(super::matches(filter1, filter2));
        assert!(!super::matches(filter2, filter1));
    }
}
