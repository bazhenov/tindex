use crate::{prelude::*, PostingList};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::str::FromStr;

pub trait Encoder {
    fn write_values(&mut self, values: impl Iterator<Item = u64>) -> IoResult<()> {
        for value in values {
            self.write(value)?;
        }
        Ok(())
    }

    fn write(&mut self, value: u64) -> IoResult<()>;
}

pub struct PlainTextEncoder(pub File);

impl Encoder for PlainTextEncoder {
    fn write(&mut self, value: u64) -> IoResult<()> {
        writeln!(&mut self.0, "{}", value)
    }
}

pub struct PlainTextDecoder(BufReader<File>);

impl PlainTextDecoder {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self(BufReader::new(File::open(path.as_ref())?)))
    }
}

impl PostingList for PlainTextDecoder {
    fn next(&mut self) -> Result<Option<u64>> {
        let mut line = String::new();
        let result = self.0.read_line(&mut line)?;
        if result == 0 {
            return Ok(None);
        }
        let n = u64::from_str(line.trim_end())?;
        Ok(Some(n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn check_plaintext_readwrite() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("plaintext.txt");

        let mut text = PlainTextEncoder(File::create(&path)?);
        text.write_values(0..10)?;

        let result = PlainTextDecoder::open(&path)?.to_vec()?;

        assert_eq!(result, (0..10).collect::<Vec<_>>());
        Ok(())
    }
}
