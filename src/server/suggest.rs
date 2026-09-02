const MAX_SUGGEST_INPUT_CHARS: usize = 256;

pub fn levenshtein(a: &str, b: &str) -> usize {
    if a == b {
        return 0;
    }
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    let (short, long) = if a.len() > b.len() { (b, a) } else { (a, b) };
    let mut prev: Vec<usize> = (0..=short.len()).collect();
    let mut curr = vec![0; short.len() + 1];
    for (j, lc) in long.iter().enumerate() {
        curr[0] = j + 1;
        for (i, sc) in short.iter().enumerate() {
            let cost = usize::from(sc != lc);
            curr[i + 1] = (curr[i] + 1).min(prev[i + 1] + 1).min(prev[i] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[short.len()]
}

/// The closest registered method name, when it is close enough to be a typo.
pub fn closest<'a>(method: &str, candidates: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    if method.is_empty() || method.len() > MAX_SUGGEST_INPUT_CHARS {
        return None;
    }
    let normalized = method.to_lowercase();
    let threshold = 2.max(method.len() * 2 / 5);
    let mut best: Option<(&str, usize)> = None;
    for candidate in candidates {
        if candidate.len().abs_diff(normalized.len()) > threshold {
            continue;
        }
        let score = levenshtein(&normalized, &candidate.to_lowercase());
        if score <= threshold && best.is_none_or(|(_, b)| score < b) {
            best = Some((candidate, score));
            if score == 0 {
                break;
            }
        }
    }
    best.map(|(name, _)| name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suggests_near_misses_only() {
        let names = ["user.getById", "user.create", "order.place"];
        assert_eq!(closest("user.getByI", names), Some("user.getById"));
        assert_eq!(closest("user.Create", names), Some("user.create"));
        assert_eq!(closest("payments.refund", names), None);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
    }
}
