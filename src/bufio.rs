use std::io::{BufReader, BufWriter, Read, Write};

#[derive(Debug)]
pub(super) struct BufReaderWithPos<R>
where
    R: Read,
{
    pos: u64,
    reader: BufReader<R>,
}

#[derive(Debug)]
pub(super) struct BufWriterWithPos<W>
where
    W: Write,
{
    pos: u64,
    writer: BufWriter<W>,
}
