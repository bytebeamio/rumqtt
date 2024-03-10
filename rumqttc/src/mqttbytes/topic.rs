/// Maximum length of a topic or topic filter according to
/// [MQTT-4.7.3-3](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718109)
pub const MAX_TOPIC_LEN: usize = 65535;

/// Checks if a topic or topic filter has wildcards
pub fn has_wildcards(s: impl AsRef<str>) -> bool {
    s.as_ref().contains(['+', '#'])
}

/// Check if a topic is valid for PUBLISH packet.
pub fn valid_topic(topic: impl AsRef<str>) -> bool {
    is_valid_topic_or_filter(&topic) && !has_wildcards(topic)
}

/// Check if a topic is valid to qualify as a topic name or topic filter.
///
/// According to MQTT v3 Spec, it has to follow the following rules:
/// 1. All Topic Names and Topic Filters MUST be at least one character long [MQTT-4.7.3-1]
/// 2. Topic Names and Topic Filters are case sensitive
/// 3. Topic Names and Topic Filters can include the space character
/// 4. A leading or trailing `/` creates a distinct Topic Name or Topic Filter
/// 5. A Topic Name or Topic Filter consisting only of the `/` character is valid
/// 6. Topic Names and Topic Filters MUST NOT include the null character (Unicode U+0000) [MQTT-4.7.3-2]
/// 7. Topic Names and Topic Filters are UTF-8 encoded strings, they MUST NOT encode to more than 65535 bytes.
fn is_valid_topic_or_filter(topic_or_filter: impl AsRef<str>) -> bool {
    let topic_or_filter = topic_or_filter.as_ref();
    if topic_or_filter.is_empty()
        || topic_or_filter.len() > MAX_TOPIC_LEN
        || topic_or_filter.contains('\0')
    {
        return false;
    }

    true
}

/// Checks if the filter is valid
///
/// <https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106>
pub fn valid_filter(filter: impl AsRef<str>) -> bool {
    let filter = filter.as_ref();
    if !is_valid_topic_or_filter(filter) {
        return false;
    }

    // rev() is used so we can easily get the last entry
    let mut hirerarchy = filter.split('/').rev();

    // split will never return an empty iterator
    // even if the pattern isn't matched, the original string will be there
    // so it is safe to just unwrap here!
    let last = hirerarchy.next().unwrap();

    // only single '#" or '+' is allowed in last entry
    // invalid: sport/tennis#
    // invalid: sport/++
    if last.len() != 1 && (last.contains('#') || last.contains('+')) {
        return false;
    }

    // remaining entries
    for entry in hirerarchy {
        // # is not allowed in filter except as a last entry
        // invalid: sport/tennis#/player
        // invalid: sport/tennis/#/ranking
        if entry.contains('#') {
            return false;
        }

        // + must occupy an entire level of the filter
        // invalid: sport+
        if entry.len() > 1 && entry.contains('+') {
            return false;
        }
    }

    true
}

/// Checks if topic matches a filter. topic and filter validation isn't done here.
///
/// **NOTE**: 'topic' is a misnomer in the arg. this can also be used to match 2 wild subscriptions
/// **NOTE**: make sure a topic is validated during a publish and filter is validated
/// during a subscribe
pub fn matches(topic: impl AsRef<str>, filter: impl AsRef<str>) -> bool {
    let topic = topic.as_ref();
    let filter = filter.as_ref();

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
            Some("#") => return false,
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
        assert!(!super::valid_filter("wr/o+/ng"));
        assert!(!super::valid_filter("wr/+o+/ng"));
        assert!(!super::valid_filter("wron/+g"));
        assert!(super::valid_filter("cor/+/rect/+"));
    }

    #[test]
    fn zero_len_subscriptions_are_not_allowed() {
        assert!(!super::valid_filter(""));
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

        let filter1 = "a/b/c/d/e";
        let filter2 = "a/+/+/+/e";
        assert!(super::matches(filter1, filter2));

        let filter1 = "a/+/c/+/e";
        let filter2 = "a/+/+/+/e";
        assert!(super::matches(filter1, filter2));

        let filter1 = "a/+/+/+/e";
        let filter2 = "a/+/+/+/e";
        assert!(super::matches(filter1, filter2));
    }
}
