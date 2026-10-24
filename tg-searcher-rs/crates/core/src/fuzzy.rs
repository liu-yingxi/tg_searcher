//! Fuzzy text matching used as a fallback when exact/boolean search returns
//! too little. The similarity score is independent of the search engine, so
//! the threshold can be tuned (or overridden per query) without touching the
//! index implementation.
//!
//! Two complementary signals are combined:
//!
//! * weighted token coverage — each query token (jieba segmentation, search
//!   mode) is matched against the best message token, using substring or
//!   normalised Levenshtein similarity;
//! * character-bigram Dice coefficient over the best matching sentence.
//!
//! `score = max(coverage, dice)`, filtered by `threshold`.

use std::collections::HashMap;
use std::sync::OnceLock;

use jieba_rs::Jieba;

use crate::config::FuzzyConfig;

fn jieba() -> &'static Jieba {
    static JIEBA: OnceLock<Jieba> = OnceLock::new();
    JIEBA.get_or_init(Jieba::new)
}

/// Tokens emitted by the index tokenizer, with byte offsets into the original
/// text. Search mode produces overlapping sub-words, which improves recall for
/// Chinese queries.
pub fn index_tokens(text: &str) -> Vec<(String, usize, usize)> {
    let mut out = Vec::new();
    for token in jieba().cut_for_search(text, true) {
        let word = token.word.trim();
        if word.is_empty() || !is_meaningful(word) {
            continue;
        }
        out.push((
            word.to_lowercase(),
            token.byte_start,
            token.byte_end.min(text.len()),
        ));
    }
    out
}

/// Tokens used when converting a user query to an exact query. Default mode
/// avoids the overlapping sub-words of search mode; the index still contains
/// them, so a short query term matches the corresponding indexed sub-word.
pub fn query_tokens(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in jieba().cut(text, true) {
        let word = token.word.trim();
        if word.is_empty() || !is_meaningful(word) {
            continue;
        }
        let lowered = word.to_lowercase();
        if !out.contains(&lowered) {
            out.push(lowered);
        }
    }
    out
}

/// Lowercase and collapse whitespace. CJK is left untouched.
pub fn normalize(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut last_space = false;
    for ch in text.chars().flat_map(char::to_lowercase) {
        if ch.is_whitespace() {
            if !last_space && !out.is_empty() {
                out.push(' ');
            }
            last_space = true;
        } else {
            out.push(ch);
            last_space = false;
        }
    }
    out.trim().to_string()
}

fn is_meaningful(token: &str) -> bool {
    token.chars().any(|c| c.is_alphanumeric() || is_cjk(c))
}

fn is_cjk(c: char) -> bool {
    matches!(c as u32,
        0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF | 0x3040..=0x30FF | 0xAC00..=0xD7AF)
}

/// jieba segmentation in search mode, lowercase, punctuation removed.
pub fn segment(text: &str) -> Vec<String> {
    let lowered = text.to_lowercase();
    let mut out = Vec::new();
    for token in jieba().cut_for_search(&lowered, true) {
        let word = token.word.trim();
        if word.is_empty() || !is_meaningful(word) {
            continue;
        }
        // Drop tokens that are a single punctuation-like symbol.
        out.push(word.to_string());
    }
    out
}

/// Normalised Levenshtein similarity in `[0, 1]`.
pub fn levenshtein_ratio(a: &str, b: &str) -> f32 {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    let max = a.len().max(b.len());
    if max == 0 {
        return 1.0;
    }
    let dist = levenshtein(&a, &b);
    1.0 - dist as f32 / max as f32
}

fn levenshtein(a: &[char], b: &[char]) -> usize {
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            cur[j + 1] = (prev[j + 1] + 1).min(cur[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

fn char_bigrams(text: &str) -> HashMap<(char, char), usize> {
    let chars: Vec<char> = text.chars().collect();
    let mut map = HashMap::new();
    if chars.len() < 2 {
        if let Some(c) = chars.first() {
            map.insert((*c, *c), 1);
        }
        return map;
    }
    for pair in chars.windows(2) {
        *map.entry((pair[0], pair[1])).or_insert(0) += 1;
    }
    map
}

/// Multiset Dice coefficient in `[0, 1]`.
pub fn bigram_dice(a: &str, b: &str) -> f32 {
    let a = char_bigrams(a);
    let b = char_bigrams(b);
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    let total = a.values().sum::<usize>() + b.values().sum::<usize>();
    if total == 0 {
        return 0.0;
    }
    let mut overlap = 0usize;
    for (gram, count) in &a {
        if let Some(other) = b.get(gram) {
            overlap += (*count).min(*other);
        }
    }
    2.0 * overlap as f32 / total as f32
}

fn best_segment_dice(query: &str, content: &str) -> f32 {
    // Score against sentence-like chunks, so that a short query inside a long
    // message is not diluted by the whole document.
    let mut best: f32 = 0.0;
    let mut segments = 0;
    let mut current = String::new();
    let flush = |current: &mut String, best: &mut f32, segments: &mut usize| {
        let chunk = current.trim();
        if !chunk.is_empty() && *segments < 64 {
            let dice = bigram_dice(query, chunk);
            if dice > *best {
                *best = dice;
            }
            *segments += 1;
        }
        current.clear();
    };
    for ch in content.chars().take(4000) {
        if matches!(
            ch,
            '。' | '！'
                | '？'
                | '；'
                | '，'
                | '.'
                | '!'
                | '?'
                | ';'
                | ','
                | '\n'
                | '\r'
                | '\t'
                | '：'
                | ':'
        ) {
            flush(&mut current, &mut best, &mut segments);
        } else {
            current.push(ch);
        }
    }
    flush(&mut current, &mut best, &mut segments);
    best
}

fn token_best_score(query_token: &str, content_tokens: &[String], content_norm: &str) -> f32 {
    if query_token.chars().count() <= 3 && content_norm.contains(query_token) {
        return 1.0;
    }
    let mut best: f32 = 0.0;
    for candidate in content_tokens {
        if candidate == query_token {
            return 1.0;
        }
        if query_token.chars().count() >= 2 && candidate.contains(query_token) {
            return 1.0;
        }
        if candidate.chars().count() >= 2 && query_token.contains(candidate.as_str()) {
            return 1.0;
        }
        let score = levenshtein_ratio(query_token, candidate);
        if score > best {
            best = score;
        }
    }
    if content_norm.contains(query_token) {
        return 1.0;
    }
    best
}

/// Compute the fuzzy similarity of `query` against message content. The
/// filename, when present, participates with the same weight as content.
pub fn similarity(query: &str, content: &str, filename: Option<&str>) -> f32 {
    let query = normalize(query);
    if query.is_empty() {
        return 0.0;
    }
    let mut haystack = normalize(content);
    if let Some(filename) = filename {
        if !haystack.is_empty() {
            haystack.push(' ');
        }
        haystack.push_str(&normalize(filename));
    }
    if haystack.is_empty() {
        return 0.0;
    }
    if haystack.contains(&query) {
        return 1.0;
    }

    let qtokens = segment(&query);
    let ctokens = segment(&haystack);

    let mut coverage = 0.0f32;
    if !qtokens.is_empty() {
        let mut total_weight = 0.0f32;
        for token in &qtokens {
            let weight = token.chars().count().max(1) as f32;
            total_weight += weight;
            coverage += weight * token_best_score(token, &ctokens, &haystack);
        }
        if total_weight > 0.0 {
            coverage /= total_weight;
        }
    }

    let dice = best_segment_dice(&query, &haystack);
    coverage.max(dice).clamp(0.0, 1.0)
}

/// Whether a candidate passes the configured threshold.
pub fn passes(query: &str, content: &str, filename: Option<&str>, threshold: f32) -> bool {
    similarity(query, content, filename) + f32::EPSILON >= threshold.clamp(0.0, 1.0)
}

/// Convenience wrapper that also respects `fuzzy.enabled`.
pub fn candidate_similarity(
    query: &str,
    content: &str,
    filename: Option<&str>,
    cfg: &FuzzyConfig,
) -> Option<f32> {
    if !cfg.enabled || query.chars().count() < cfg.min_query_chars {
        return None;
    }
    let score = similarity(query, content, filename);
    if score + f32::EPSILON >= cfg.effective_threshold() {
        Some(score)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FuzzyMode;

    #[test]
    fn normalization() {
        assert_eq!(normalize("  Hello   WORLD\n"), "hello world");
        assert_eq!(normalize("中文　空格"), "中文 空格");
    }

    #[test]
    fn segmentation_keeps_chinese_and_english() {
        let tokens = segment("机器学习 machine learning");
        assert!(tokens.iter().any(|t| t.contains("学习")));
        assert!(tokens.iter().any(|t| t == "machine"));
    }

    #[test]
    fn exact_substring_scores_one() {
        assert_eq!(similarity("搜索", "全文搜索引擎", None), 1.0);
        assert_eq!(
            similarity("machine learning", "Intro to Machine Learning", None),
            1.0
        );
    }

    #[test]
    fn related_chinese_scores_above_half() {
        let s = similarity("机器学习", "深度学习入门", None);
        assert!(s >= 0.5, "score was {s}");
        let s = similarity("搜素引擎", "搜索引擎", None);
        assert!(s >= 0.7, "score was {s}");
    }

    #[test]
    fn unrelated_scores_low() {
        let s = similarity("天气预报", "机器学习笔记", None);
        assert!(s < 0.3, "score was {s}");
    }

    #[test]
    fn english_typo_scores_high() {
        let s = similarity("machin lerning", "machine learning", None);
        assert!(s >= 0.8, "score was {s}");
    }

    #[test]
    fn threshold_is_decoupled_and_effective() {
        let content = "深度学习入门";
        let query = "机器学习";
        let score = similarity(query, content, None);
        assert!(passes(query, content, None, 0.5));
        let mut cfg = FuzzyConfig {
            threshold: 0.99,
            ..Default::default()
        };
        assert!(candidate_similarity(query, content, None, &cfg).is_none());
        cfg.threshold = (score - 0.01).max(0.0);
        assert!(candidate_similarity(query, content, None, &cfg).is_some());
        cfg.enabled = false;
        assert!(candidate_similarity(query, content, None, &cfg).is_none());
    }

    #[test]
    fn filename_participates() {
        let s = similarity("季度报告", "", Some("2024年季度报告.pdf"));
        assert_eq!(s, 1.0);
    }

    #[test]
    fn mode_default_is_fallback() {
        assert_eq!(FuzzyConfig::default().mode, FuzzyMode::Fallback);
    }
}
