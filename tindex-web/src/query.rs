//! Токенизация и парсинг запросов
//!
//! Для токенизации используется библиотека [PEST](https://github.com/pest-parser/pest).
use crate::{prelude::*, Index};
use anyhow::bail;
use fn_error_context::context;
use pest::{iterators::Pairs, Parser};
use pest_derive::Parser;
use tindex_core::{Exclude, Merge, PostingList};

#[derive(Parser)]
#[grammar = "grammar.pest"]
struct QueryParser;

#[derive(Debug, PartialEq)]
enum Ast {
    Exclude(Box<Ast>, Box<Ast>),
    Merge(Box<Ast>, Box<Ast>),
    Intersect(Box<Ast>, Box<Ast>),
    Ident(String),
}

/// Выполняет парсинг запроса
///
/// Возвращает [PostingList] готовый к итерации. Индивидуальные термы по имени ищутся в переданном экземпляре [Index].
#[context("Parsing query: {}", query)]
pub fn parse_query(query: &str, index: &impl Index) -> Result<PostingList> {
    let tokens = QueryParser::parse(Rule::root, query)?;
    visit(parse_ast(tokens)?, index)
}

fn visit(node: Ast, index: &impl Index) -> Result<PostingList> {
    let result: PostingList = match node {
        Ast::Ident(name) => index.lookup(&name)?.into(),
        Ast::Exclude(lv, rv) => Exclude(visit(*lv, index)?, visit(*rv, index)?).into(),
        Ast::Merge(lv, rv) => Merge(visit(*lv, index)?, visit(*rv, index)?).into(),
        Ast::Intersect(_lv, _rv) => unimplemented!(),
    };
    Ok(result)
}

fn parse_ast(input: Pairs<Rule>) -> Result<Ast> {
    let mut expr = None;
    let mut op = None;

    for pair in input {
        if let Rule::OP = pair.as_rule() {
            op = Some(pair.as_str().to_string());
        } else {
            let rv = match pair.as_rule() {
                Rule::expression | Rule::root => parse_ast(pair.into_inner())?,
                Rule::ident => Ast::Ident(pair.as_str().to_string()),
                Rule::EOI => break,
                s => bail!("expression or ident expected, {:?} found", s),
            };
            expr = if let Some(lv) = expr {
                let lv = Box::new(lv);
                let rv = Box::new(rv);
                match op.as_ref().map(String::as_ref) {
                    Some("&") => Some(Ast::Intersect(lv, rv)),
                    Some("|") => Some(Ast::Merge(lv, rv)),
                    Some("-") => Some(Ast::Exclude(lv, rv)),
                    _ => bail!("Invalid index operation"),
                }
            } else {
                Some(rv)
            }
        }
    }
    if let Some(e) = expr {
        Ok(e)
    } else {
        bail!("No expression found")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() -> Result<()> {
        let tokens = QueryParser::parse(Rule::expression, "(a1 & b1) - (u1 | a1 | u3)")?;

        let intersect = Ast::Intersect(
            Box::new(Ast::Ident("a1".to_string())),
            Box::new(Ast::Ident("b1".to_string())),
        );
        let merge = Ast::Merge(
            Box::new(Ast::Ident("u1".to_string())),
            Box::new(Ast::Merge(
                Box::new(Ast::Ident("a1".to_string())),
                Box::new(Ast::Ident("u3".to_string())),
            )),
        );
        let expected = Ast::Exclude(Box::new(intersect), Box::new(merge));

        assert_eq!(parse_ast(tokens)?, expected);
        Ok(())
    }
}
