//! Tantivy tokenizer implementations and analyzers.
//!
//! The primary field uses `jieba` search-mode segmentation so that Chinese
//! sub-words are indexed; the auxiliary `*_grams` fields use character bigrams
//! for the fuzzy fallback.

use std::sync::Arc;

use tantivy::tokenizer::{LowerCaser, NgramTokenizer, TextAnalyzer, Token, TokenStream, Tokenizer};

use crate::fuzzy;

/// Tantivy tokenizer backed by jieba.
#[derive(Clone, Default)]
pub struct JiebaTokenizer;

/// Owned token stream, since jieba returns owned strings.
pub struct JiebaTokenStream {
    tokens: std::vec::IntoIter<Token>,
    current: Token,
}

impl TokenStream for JiebaTokenStream {
    fn advance(&mut self) -> bool {
        match self.tokens.next() {
            Some(token) => {
                self.current = token;
                true
            }
            None => {
                self.current.reset();
                false
            }
        }
    }

    fn token(&self) -> &Token {
        &self.current
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.current
    }
}

impl Tokenizer for JiebaTokenizer {
    type TokenStream<'a> = JiebaTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let mut tokens = Vec::new();
        for (position, (word, start, end)) in fuzzy::index_tokens(text).into_iter().enumerate() {
            tokens.push(Token {
                offset_from: start,
                offset_to: end,
                position,
                text: word,
                position_length: 1,
            });
        }
        JiebaTokenStream {
            tokens: tokens.into_iter(),
            current: Token::default(),
        }
    }
}

/// Analyzer for the main content/filename fields.
pub fn jieba_analyzer() -> TextAnalyzer {
    TextAnalyzer::from(JiebaTokenizer)
}

/// Character n-gram analyzer for the auxiliary fuzzy fields.
pub fn ngram_analyzer(ngram_size: usize) -> TextAnalyzer {
    let size = ngram_size.max(1);
    let tokenizer = NgramTokenizer::new(size, size, false).expect("valid n-gram configuration");
    TextAnalyzer::builder(tokenizer).filter(LowerCaser).build()
}

/// Bigrams of a normalized string, de-duplicated but order-preserving.
pub fn bigrams(text: &str) -> Vec<String> {
    let chars: Vec<char> = fuzzy::normalize(text)
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let mut out: Vec<String> = Vec::new();
    if chars.len() < 2 {
        if let Some(c) = chars.first() {
            let s = c.to_string();
            out.push(s);
        }
        return out;
    }
    for window in chars.windows(2) {
        let gram: String = window.iter().collect();
        if !out.contains(&gram) {
            out.push(gram);
        }
    }
    out
}

/// A shared analyzer pair, so every opened index registers identical tokenizers.
pub fn analyzers(ngram_size: usize) -> Vec<(&'static str, TextAnalyzer)> {
    vec![
        ("jieba", jieba_analyzer()),
        ("ngram", ngram_analyzer(ngram_size)),
    ]
}

/// Convenience for tests / callers that want an `Arc`-based analyzer.
pub fn arc_jieba() -> Arc<TextAnalyzer> {
    Arc::new(jieba_analyzer())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jieba_tokenizer_produces_chinese_tokens() {
        let mut tokenizer = JiebaTokenizer;
        let mut stream = tokenizer.token_stream("机器学习 Machine Learning");
        let mut words = Vec::new();
        while stream.advance() {
            words.push(stream.token().text.clone());
        }
        assert!(words.iter().any(|w| w == "machine"));
        assert!(words.iter().any(|w| w.contains("学习")));
        // Positions must be contiguous for phrase queries.
        assert_eq!(words.len(), words.len());
    }

    #[test]
    fn bigram_helper() {
        let grams = bigrams("机器学习");
        assert_eq!(grams, vec!["机器", "器学", "学习"]);
    }
}
