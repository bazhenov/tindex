use crate::prelude::*;
use pest::{iterators::Pair, Parser};

#[derive(Parser)]
#[grammar = "grammar.pest"]
struct QueryParser;

#[derive(Debug)]
enum Ast {
    Exclude(Box<Ast>, Box<Ast>),
    Merge(Box<Ast>, Box<Ast>),
    Intersect(Box<Ast>, Box<Ast>),
    Ident(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() -> Result<()> {
        let mut pairs =
            QueryParser::parse(Rule::root, "(a1 & b1) - u1").unwrap_or_else(|e| panic!("{}", e));
        let ast = parse_ast(pairs.next().unwrap().into_inner().next().unwrap())?;
        // println!("{:?}", ast);

        Ok(())
    }
}

fn parse_ast(input: Pair<Rule>) -> Result<Ast> {
    match input.as_rule() {
        Rule::expression => {
            println!("Expression: {:?}", input.as_str());
            let mut i = input.into_inner();
            let a = parse_ast(i.next().unwrap())?;
            let b = parse_ast(i.next().unwrap())?;
            Ok(Ast::Intersect(Box::new(a), Box::new(b)))
        }
        Rule::ident => Ok(Ast::Ident(input.as_str().to_string())),
        _ => {
            panic!("Invalid type {:?}", input)
        }
    }
}
