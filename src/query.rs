use crate::{
    encoding::PlainTextDecoder, prelude::*, Exclude, Intersect, Merge, PositionedPostingList,
    PostingList,
};
use anyhow::{bail, Context};
use pest::{iterators::Pairs, Parser};
use std::{fs::File, io::BufReader, path::PathBuf};

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

pub struct PostingLists(pub PathBuf);

impl PostingLists {
    pub fn parse_query(&self, query: &str) -> Result<Box<dyn PostingList>> {
        let tokens = QueryParser::parse(Rule::root, query)
            .context(Error::InvalidQuery(query.to_string()))?;
        let ast = parse_ast(tokens)?;
        self.visit(ast)
    }

    fn visit(&self, node: Ast) -> Result<Box<dyn PostingList>> {
        match node {
            Ast::Ident(name) => {
                let path = self.0.join(format!("{}.idx", name));
                let file = File::open(&path).context(Error::UnableToOpenPostingListFile(path))?;

                Ok(Box::new(PlainTextDecoder(BufReader::new(file))))
            }
            Ast::Exclude(lv, rv) => Ok(Box::new(Exclude(
                PositionedPostingList(self.visit(*lv)?, None),
                PositionedPostingList(self.visit(*rv)?, None),
            ))),
            Ast::Merge(lv, rv) => Ok(Box::new(Merge(
                PositionedPostingList(self.visit(*lv)?, None),
                PositionedPostingList(self.visit(*rv)?, None),
            ))),
            Ast::Intersect(lv, rv) => Ok(Box::new(Intersect(
                PositionedPostingList(self.visit(*lv)?, None),
                PositionedPostingList(self.visit(*rv)?, None),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() -> Result<()> {
        let pairs = QueryParser::parse(Rule::expression, "(a1 & b1) - (u1 | a1 | u3)")
            .unwrap_or_else(|e| panic!("{}", e));
        let ast = parse_ast(pairs)?;

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

        assert_eq!(ast, expected);
        Ok(())
    }
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
                    Some(s) => bail!("Invalid op '{}'", s),
                    None => bail!("No op found"),
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
