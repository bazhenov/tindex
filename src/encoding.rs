use crate::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::str::FromStr;

trait Encoder {
    fn write(&mut self, values: impl Iterator<Item = u64>) -> IoResult<()>;
}

trait Decoder {
    fn read(&mut self) -> Result<Option<u64>>;
}

pub struct PlainTextEncoder(File);

impl Encoder for PlainTextEncoder {
    fn write(&mut self, values: impl Iterator<Item = u64>) -> IoResult<()> {
        for value in values {
            writeln!(&mut self.0, "{}", value)?;
        }
        Ok(())
    }
}

pub struct PlainTextDecoder(BufReader<File>);

impl PlainTextDecoder {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self(BufReader::new(File::open(path.as_ref())?)))
    }
}

impl Decoder for PlainTextDecoder {
    fn read(&mut self) -> Result<Option<u64>> {
        let mut line = String::new();
        let result = self.0.read_line(&mut line)?;
        if result == 0 {
            return Ok(None);
        }
        let n = u64::from_str(line.trim_end())?;
        return Ok(Some(n));
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
        text.write(0..10)?;

        let mut text = PlainTextDecoder::open(&path)?;

        let mut result = vec![];
        while let Some(n) = text.read()? {
            result.push(n);
        }

        assert_eq!(result, (0..10).collect::<Vec<_>>());
        Ok(())
    }
}
