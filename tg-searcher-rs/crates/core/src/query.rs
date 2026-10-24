//! A small, dependency-free query parser compatible with the subset of the
//! Whoosh query language advertised by the original project:
//!
//! * bare terms, implicitly AND-ed;
//! * `AND`, `OR`, `NOT` (also `&&`, `||`, `!`);
//! * parentheses;
//! * quoted phrases;
//! * `*` / `?` wildcards inside a term;
//! * a lone `*` matches everything.
//!
//! The parser produces a neutral AST; [`crate::index`] converts it to tantivy
//! queries.

use std::fmt;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryNode {
    All,
    Term(String),
    Wildcard(String),
    Phrase(String),
    Not(Box<QueryNode>),
    And(Vec<QueryNode>),
    Or(Vec<QueryNode>),
}

impl QueryNode {
    pub fn and(mut children: Vec<QueryNode>) -> Self {
        children.retain(|c| !matches!(c, QueryNode::All));
        match children.len() {
            0 => QueryNode::All,
            1 => children.pop().unwrap(),
            _ => QueryNode::And(children),
        }
    }

    pub fn or(mut children: Vec<QueryNode>) -> Self {
        match children.len() {
            0 => QueryNode::All,
            1 => children.pop().unwrap(),
            _ => QueryNode::Or(children),
        }
    }

    /// Text used to seed the fuzzy fallback: all positive terms and phrases,
    /// with wildcards stripped and negation dropped.
    pub fn fallback_text(&self) -> String {
        let mut parts = Vec::new();
        self.collect_positive(&mut parts);
        parts.join(" ")
    }

    fn collect_positive(&self, out: &mut Vec<String>) {
        match self {
            QueryNode::All => {}
            QueryNode::Term(t) | QueryNode::Phrase(t) | QueryNode::Wildcard(t) => {
                let cleaned = t.replace(['*', '?'], " ").trim().to_string();
                if !cleaned.is_empty() {
                    out.push(cleaned);
                }
            }
            QueryNode::Not(_) => {}
            QueryNode::And(children) | QueryNode::Or(children) => {
                for child in children {
                    child.collect_positive(out);
                }
            }
        }
    }

    /// True when this AST is a pure "match everything" query.
    pub fn is_all(&self) -> bool {
        matches!(self, QueryNode::All)
    }
}

impl fmt::Display for QueryNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryNode::All => write!(f, "*"),
            QueryNode::Term(t) => write!(f, "{t}"),
            QueryNode::Wildcard(t) => write!(f, "{t}"),
            QueryNode::Phrase(t) => write!(f, "\"{t}\""),
            QueryNode::Not(inner) => write!(f, "NOT ({inner})"),
            QueryNode::And(children) => {
                let parts: Vec<String> = children.iter().map(ToString::to_string).collect();
                write!(f, "({})", parts.join(" AND "))
            }
            QueryNode::Or(children) => {
                let parts: Vec<String> = children.iter().map(ToString::to_string).collect();
                write!(f, "({})", parts.join(" OR "))
            }
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum QueryError {
    #[error("query is empty")]
    Empty,
    #[error("unbalanced parentheses")]
    UnbalancedParens,
    #[error("unexpected token `{0}` at position {1}")]
    UnexpectedToken(String, usize),
    #[error("expected a search term at position {0}")]
    ExpectedTerm(usize),
    #[error("unterminated quoted phrase")]
    UnterminatedPhrase,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LexToken {
    LParen(usize),
    RParen(usize),
    And,
    Or,
    Not,
    Phrase(String),
    Word(String),
}

struct Lexer<'a> {
    chars: Vec<char>,
    pos: usize,
    _input: &'a str,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            chars: input.chars().collect(),
            pos: 0,
            _input: input,
        }
    }

    fn tokens(mut self) -> Result<Vec<LexToken>, QueryError> {
        let mut tokens = Vec::new();
        while self.pos < self.chars.len() {
            let c = self.chars[self.pos];
            match c {
                c if c.is_whitespace() => self.pos += 1,
                '(' => {
                    tokens.push(LexToken::LParen(self.pos));
                    self.pos += 1;
                }
                ')' => {
                    tokens.push(LexToken::RParen(self.pos));
                    self.pos += 1;
                }
                '"' => {
                    let start = self.pos;
                    self.pos += 1;
                    let mut phrase = String::new();
                    let mut closed = false;
                    while self.pos < self.chars.len() {
                        match self.chars[self.pos] {
                            '\\' if self.pos + 1 < self.chars.len() => {
                                phrase.push(self.chars[self.pos + 1]);
                                self.pos += 2;
                            }
                            '"' => {
                                self.pos += 1;
                                closed = true;
                                break;
                            }
                            ch => {
                                phrase.push(ch);
                                self.pos += 1;
                            }
                        }
                    }
                    if !closed {
                        return Err(QueryError::UnterminatedPhrase);
                    }
                    let phrase = phrase.trim().to_string();
                    if phrase.is_empty() {
                        return Err(QueryError::UnexpectedToken("\"\"".into(), start));
                    }
                    tokens.push(LexToken::Phrase(phrase));
                }
                '&' if self.peek(1) == Some('&') => {
                    tokens.push(LexToken::And);
                    self.pos += 2;
                }
                '|' if self.peek(1) == Some('|') => {
                    tokens.push(LexToken::Or);
                    self.pos += 2;
                }
                '!' => {
                    tokens.push(LexToken::Not);
                    self.pos += 1;
                }
                _ => {
                    let start = self.pos;
                    let mut word = String::new();
                    while self.pos < self.chars.len() {
                        let ch = self.chars[self.pos];
                        if ch.is_whitespace() || matches!(ch, '(' | ')' | '"') {
                            break;
                        }
                        word.push(ch);
                        self.pos += 1;
                    }
                    if word.is_empty() {
                        return Err(QueryError::UnexpectedToken(c.to_string(), start));
                    }
                    match word.as_str() {
                        "AND" => tokens.push(LexToken::And),
                        "OR" => tokens.push(LexToken::Or),
                        "NOT" => tokens.push(LexToken::Not),
                        _ => tokens.push(LexToken::Word(word)),
                    }
                }
            }
        }
        Ok(tokens)
    }

    fn peek(&self, offset: usize) -> Option<char> {
        self.chars.get(self.pos + offset).copied()
    }
}

struct Parser {
    tokens: Vec<LexToken>,
    index: usize,
}

impl Parser {
    fn new(tokens: Vec<LexToken>) -> Self {
        Self { tokens, index: 0 }
    }

    fn peek(&self) -> Option<&LexToken> {
        self.tokens.get(self.index)
    }

    fn next(&mut self) -> Option<LexToken> {
        let token = self.tokens.get(self.index).cloned();
        if token.is_some() {
            self.index += 1;
        }
        token
    }

    fn parse_or(&mut self) -> Result<QueryNode, QueryError> {
        let mut children = vec![self.parse_and()?];
        while matches!(self.peek(), Some(LexToken::Or)) {
            self.next();
            children.push(self.parse_and()?);
        }
        Ok(QueryNode::or(children))
    }

    fn parse_and(&mut self) -> Result<QueryNode, QueryError> {
        let mut children = vec![self.parse_not()?];
        loop {
            match self.peek() {
                Some(LexToken::And) => {
                    self.next();
                    children.push(self.parse_not()?);
                }
                // Implicit AND between adjacent operands.
                Some(LexToken::Word(_))
                | Some(LexToken::Phrase(_))
                | Some(LexToken::LParen(_))
                | Some(LexToken::Not) => {
                    children.push(self.parse_not()?);
                }
                _ => break,
            }
        }
        Ok(QueryNode::and(children))
    }

    fn parse_not(&mut self) -> Result<QueryNode, QueryError> {
        if matches!(self.peek(), Some(LexToken::Not)) {
            self.next();
            let inner = self.parse_not()?;
            return Ok(QueryNode::Not(Box::new(inner)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<QueryNode, QueryError> {
        let token = self.next().ok_or(QueryError::ExpectedTerm(0))?;
        match token {
            LexToken::Word(word) => {
                if word == "*" {
                    Ok(QueryNode::All)
                } else if word.contains('*') || word.contains('?') {
                    Ok(QueryNode::Wildcard(word))
                } else {
                    Ok(QueryNode::Term(word))
                }
            }
            LexToken::Phrase(phrase) => Ok(QueryNode::Phrase(phrase)),
            LexToken::LParen(_) => {
                let inner = self.parse_or()?;
                match self.next() {
                    Some(LexToken::RParen(_)) => Ok(inner),
                    _ => Err(QueryError::UnbalancedParens),
                }
            }
            LexToken::RParen(pos) => Err(QueryError::UnexpectedToken(")".into(), pos)),
            LexToken::And | LexToken::Or | LexToken::Not => {
                Err(QueryError::ExpectedTerm(self.index))
            }
        }
    }
}

/// Parse a user query. A query that is only whitespace is an error; callers
/// should handle it before invoking the indexer.
pub fn parse_query(input: &str) -> Result<QueryNode, QueryError> {
    let tokens = Lexer::new(input).tokens()?;
    if tokens.is_empty() {
        return Err(QueryError::Empty);
    }
    let mut parser = Parser::new(tokens);
    let node = parser.parse_or()?;
    if parser.index != parser.tokens.len() {
        let pos = parser.index;
        let repr = parser
            .tokens
            .get(pos)
            .map(|t| format!("{t:?}"))
            .unwrap_or_else(|| "end of query".into());
        return Err(QueryError::UnexpectedToken(repr, pos));
    }
    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn term(s: &str) -> QueryNode {
        QueryNode::Term(s.into())
    }

    #[test]
    fn implicit_and() {
        assert_eq!(
            parse_query("foo bar").unwrap(),
            QueryNode::And(vec![term("foo"), term("bar")])
        );
        assert_eq!(parse_query("foo").unwrap(), term("foo"));
    }

    #[test]
    fn quoted_phrase_and_rest() {
        assert_eq!(
            parse_query("\"foo bar\" baz").unwrap(),
            QueryNode::And(vec![QueryNode::Phrase("foo bar".into()), term("baz")])
        );
    }

    #[test]
    fn boolean_precedence() {
        assert_eq!(
            parse_query("NOT foo AND (bar OR woo)").unwrap(),
            QueryNode::And(vec![
                QueryNode::Not(Box::new(term("foo"))),
                QueryNode::Or(vec![term("bar"), term("woo")]),
            ])
        );
        assert_eq!(
            parse_query("a OR b AND c").unwrap(),
            QueryNode::Or(vec![term("a"), QueryNode::And(vec![term("b"), term("c")])])
        );
    }

    #[test]
    fn wildcards_and_all() {
        assert_eq!(
            parse_query("foo* ba?r").unwrap(),
            QueryNode::And(vec![
                QueryNode::Wildcard("foo*".into()),
                QueryNode::Wildcard("ba?r".into())
            ])
        );
        assert_eq!(parse_query("*").unwrap(), QueryNode::All);
    }

    #[test]
    fn errors() {
        assert_eq!(parse_query("   ").unwrap_err(), QueryError::Empty);
        assert_eq!(
            parse_query("(foo").unwrap_err(),
            QueryError::UnbalancedParens
        );
        assert_eq!(
            parse_query("\"foo").unwrap_err(),
            QueryError::UnterminatedPhrase
        );
        assert!(parse_query("foo AND").is_err());
    }

    #[test]
    fn fallback_text_drops_operators_and_negation() {
        let q = parse_query("NOT foo AND (\"bar baz\" OR woo*)").unwrap();
        assert_eq!(q.fallback_text(), "bar baz woo");
        assert_eq!(parse_query("NOT foo").unwrap().fallback_text(), "");
    }

    #[test]
    fn display_roundtrip_is_readable() {
        let q = parse_query("foo AND (bar OR baz)").unwrap();
        assert_eq!(q.to_string(), "(foo AND (bar OR baz))");
    }
}
