use crate::{prelude::*, PostingListDecoder, NO_DOC};
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

pub struct PlainTextDecoder(pub BufReader<File>);

impl PlainTextDecoder {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self(BufReader::new(File::open(path.as_ref())?)))
    }
}

impl PostingListDecoder for PlainTextDecoder {
    fn next_batch(&mut self, buffer: &mut crate::PlBuffer) -> usize {
        fn next(decoder: &mut PlainTextDecoder) -> u64 {
            let mut line = String::new();
            let result = decoder.0.read_line(&mut line).unwrap();
            if result == 0 {
                return NO_DOC;
            }
            u64::from_str(line.trim_end()).unwrap()
        }

        for i in 0..buffer.len() {
            let doc_id = next(self);
            if doc_id == NO_DOC {
                return i;
            }
            buffer[i] = doc_id;
        }
        return buffer.len();
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
        text.write_values(1..10)?;

        let result = PlainTextDecoder::open(&path)?.to_vec();

        assert_eq!(result, (1..10).collect::<Vec<_>>());
        Ok(())
    }
}
