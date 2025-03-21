use crate::bstream::{Bstream,BstreamReader};
use std::io;
pub struct XORChunk {
    b:Bstream
}

impl XORChunk {
    pub fn new() -> XORChunk {
        let mut stream = Vec::with_capacity(128);
        stream.resize(2,0);
        XORChunk {
            b: Bstream::new(stream)
        }
    }
}

struct XORAppender {

}

pub enum Error {
    IO(io::Error),
}
struct XORIterator<'a> {
    br:BstreamReader<'a>,
    num_total:u16,
    num_read:u16,

    t:i64,
    val:f64,
    
    leading:u8,
    trailing: u8,

    t_delta:u64,
    err: Option<Error>, // todo: maybe no need this field
}

impl<'a> XORIterator<'a> {
    pub fn new(stream: &'a Vec<u8>) -> XORIterator<'a> {
        // read first 2 bytes as chunk header
        let mut br = BstreamReader::new(stream);
        let mut num_total:u16 = 0;
        let mut byt = br.read_byte().unwrap();
        num_total = num_total | byt as u16;
        byt = br.read_byte().unwrap();
        num_total = (num_total << 8) | byt as u16;
        XORIterator {
            br,
            num_total,
            num_read:0,
            t:0,
            val:0.0,
            leading:0,
            trailing:0,
            t_delta:0,
            err: None
        }
    }
    fn read_bit(&mut self) -> Result<u8, io::Error> {
        match self.br.read_bit_fast() {
            Ok(b) => Ok(b as u8),
            Err(_) => self.br.read_bit().map(|b| b as u8)
        }
    }
    fn read_bits(&mut self, n: u8) -> Result<u64, io::Error> {
        match self.br.read_bits_fast(n) {
            Ok(b) => Ok(b),
            Err(_) => self.br.read_bits(n)
        }
    }
}

impl Iterator for XORIterator<'_> {
    type Item = ();
    fn next(&mut self) -> Option<Self::Item> {
        if self.err.is_some() || self.num_read == self.num_total {
            return None;
        }
        
        // read first data point
        if self.num_read == 0 {
            match self.br.read_varint() {
                Ok(t) => {
                    self.t = t;
                },
                Err(err) => {
                    self.err = Some(Error::IO(err));
                    return None;
                }
            }
            match self.br.read_bits(64) {
                Ok(val) => {
                    self.val = f64::from_bits(val);
                },
                Err(err) => {
                    self.err = Some(Error::IO(err));
                    return None;
                }
            }
            self.num_read +=1;
            return Some(());
        }
        // read second data point
        if self.num_read = 1 {
            match self.br.read_uvarint() {
                Ok(t_delta) => {
                    self.t_delta = t_delta;
                },
                Err(err) => {
                    self.err = Some(Error::IO(err));
                    return None;
                }
            }
            self.read_value()
        }
        return  None;
    }
}

impl<'a> XORIterator<'a> {
    pub fn read_value(&mut self) -> Option<()>{
        let bit:u8;
        match self.br.read_bit_fast() {
            Ok(b) => {
                bit = b as u8;
            },
            Err(_) => {
                match self.br.read_bit() {
                    Ok(b) => {
                        bit = b as u8;
                    },
                    Err(err) => {
                        self.err = Some(Error::IO(err));
                        return None;
                    }
                }
            }
        }
        if bit == 0 {
            // do nothing
        }else {
            let bit:u8;
            match self.br.read_bit_fast() {
                Ok(b) => {
                    bit = b as u8;
                },
                Err(_) => {
                    match self.br.read_bit() {
                        Ok(b) => {
                            bit = b as u8;
                        },
                        Err(err) => {
                            self.err = Some(Error::IO(err));
                            return None;
                        }
                    }
                }
            }
            if bit == 0 {
                // reuse leading/trailing zero bits
                // do nothing
            } else {
                let bit:u8;
                match self.br.read_bits_fast(5) {
                    Ok(b) => {
                        bit = b as u8;
                    },
                    Err(_) => {
                        match self.br.read_bits(5) {
                            Ok(b) => {
                                bit = b as u8;
                            },
                            Err(err) => {
                                self.err = Some(Error::IO(err));
                                return None;
                            }
                        }
                    }
                }
                self.leading = bit;
                let mbits:u8;
                match self.br.read_bits_fast(6) {
                    Ok(b) => {
                        mbits = b as u8;
                    },
                    Err(_) => {
                        match self.br.read_bits(6) {
                            Ok(b) => {
                                mbits = b as u8;
                            },
                            Err(err) => {
                                self.err = Some(Error::IO(err));
                                return None;
                            }
                        }
                    }
                }
                if mbits == 0 {
                    mbits == 64;
                }
                self.trailing = 64 - self.leading - mbits;
            }
            let mbits = 64 - self.leading - self.trailing;
            let bits :u64;
            match self.br.read_bits_fast(mbits) {
                Ok(b) => {
                    bits = b;
                },
                Err(_) => {
                    match self.br.read_bits(mbits) {
                        Ok(b) => {
                            bits = b;
                        },
                        Err(err) => {
                            self.err = Some(Error::IO(err));
                            return None;
                        }
                    }
                }
            }
            let mut vbits = f64::to_bits(self.val);
            vbits = vbits ^ (bits << self.trailing);
            self.val = f64::from_bits(vbits);
        }
        self.num_read +=1;
        return Some(());
    }
}