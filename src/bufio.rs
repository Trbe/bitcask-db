use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

#[derive(Debug)]
pub(super) struct BufReaderWithPos<R: Read> {
    pos: u64,
    reader: BufReader<R>,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    pub(super) fn new(mut r: R) -> io::Result<Self> {
        let pos = r.stream_position()?;
        let reader = BufReader::new(r);
        Ok(Self { pos, reader })
    }
}

impl<R: Read> BufReaderWithPos<R> {
    fn read(&self) -> u64 {
        self.pos
    }
}

impl<R: Read> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf).map(|bytes_read| {
            self.pos += bytes_read as u64;
            bytes_read
        })
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.reader.seek(pos).map(|pos_num| {
            self.pos = pos_num;
            pos_num
        })
    }
}
#[derive(Debug)]
pub(super) struct BufWriterWithPos<W: Write> {
    pos: u64,
    writer: BufWriter<W>,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    pub(super) fn new(mut w: W) -> io::Result<Self> {
        let pos = w.seek(SeekFrom::End(0))?;
        let writer = BufWriter::new(w);
        Ok(Self { pos, writer })
    }
}

impl<W: Write> BufWriterWithPos<W> {
    pub(super) fn pos(&self) -> u64 {
        self.pos
    }

    pub(super) fn get_ref(&self) -> &W {
        self.writer.get_ref()
    }
}

impl<W: Write> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf).map(|bytes_written| {
            self.pos += bytes_written as u64;
            bytes_written
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.writer.seek(pos).map(|pos_num| {
            self.pos = pos_num;
            pos_num
        })
    }
}
