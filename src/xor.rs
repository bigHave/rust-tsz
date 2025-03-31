use crate::bstream::{Bstream,BstreamReader};
use std::io;
use rand::Rng;
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

    pub fn encoding() -> String {
        String::from("XOR")
    }

    pub fn iterator(&mut self) -> XORIterator {
        XORIterator::new(self.b.bytes())
    }
    pub fn appender(&mut self) -> Result<XORAppender,Error>{
        // To get an appender we must know the state it would have if we had
	    // appended all existing data from scratch.
	    // We iterate through the end and populate via the iterator's state.
        let (num,state) = {
            let bytes = self.b.bytes();
            let num = u16::from_be_bytes([bytes[0],bytes[1]]);
            let mut it = XORIterator::new(bytes);
            for _ in &mut it {}
        
            if let Some(err) = it.err {
                return Err(err);
            }
            (num, (it.t, it.val, it.t_delta, it.leading, it.trailing))
        };
        let (t, val, t_delta, leading, trailing) = state;
        let mut a = XORAppender {
            b: &mut self.b,
            t,
            v: val,
            t_delta,
            leading,
            trailing,
        };
        if num == 0 {
            a.leading = 0xff;
        }
        Ok(a)
    }
}

struct XORAppender<'a> {
    b:&'a mut Bstream,

    t:i64, // starting time stamp
    v:f64,

    t_delta:u64,

    leading:u8,
    trailing: u8,
}

impl<'a> XORAppender<'a> {
    pub fn Append(&mut self, t:i64, v:f64) {
        let mut t_delta:u64 = 0;
        let bytes = self.b.bytes();
        let num = u16::from_be_bytes([bytes[0],bytes[1]]);
        if num == 0 {
            self.b.write_varint(t);
            self.b.write_bits(v.to_bits(),64);
        } else if num == 1 {
            t_delta = (t - self.t) as u64;
            self.b.write_uvarint(t_delta);
            self.write_v_delta(v);
        } else {
            t_delta = (t- self.t) as u64;
            //let dod = (t_delta - self.t_delta) as i64;
            let dod = t_delta.wrapping_sub(self.t_delta) as i64;
            // Gorilla has a max resolution of seconds, Prometheus milliseconds.
		    // Thus we use higher value range steps with larger bit size.
            match dod {
                0 => {
                    self.b.write_bit(false);
                },
                dod if bit_range(dod,14) => {
                    self.b.write_bits(0b10,2);
                    self.b.write_bits(dod as u64,14);
                },
                dod if bit_range(dod,17) => {
                    self.b.write_bits(0b110,3);
                    self.b.write_bits(dod as u64,17);
                },
                dod if bit_range(dod,20) => {
                    self.b.write_bits(0b1110,4);
                    self.b.write_bits(dod as u64,20);
                }
                _ => {
                    self.b.write_bits(0b1111,4);
                    self.b.write_bits(dod as u64,64);
                }
            }
            self.write_v_delta(v);
        }

        self.t = t;
        self.v = v;
        // update num
        let [byt1,byt2] = u16::to_be_bytes(num +1);
        self.b.modify_first_two_bytes(byt1, byt2);
        self.t_delta = t_delta;
    }

    pub fn write_v_delta(&mut self, v:f64) {
        let v_delta = v.to_bits() ^ self.v.to_bits();
        if v_delta == 0 {
            self.b.write_bit(false);
            return;
        }
        // otherwise, write a '1' anyway
        self.b.write_bit(true);

        let mut leading = v_delta.leading_zeros() as u8;
        let trailing = v_delta.trailing_zeros() as u8;
        if leading >= 32 {
            leading = 31
        }

        if self.leading != 0xff && leading >= self.leading && trailing >= self.trailing {
            self.b.write_bit(false);
            self.b.write_bits(v_delta >> self.trailing, 64 - self.leading as i32 - self.trailing as i32);
        } else {
            self.leading = leading;
            self.trailing = trailing;
            self.b.write_bit(true);
            self.b.write_bits(leading as u64,5);
            // Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
		    // Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
		    // So instead we write out a 0 and adjust it back to 64 on unpacking.

            // with write_bits if we write overflow bits it will just write 0
            let sigbits = 64 - leading - trailing;
            self.b.write_bits(sigbits as u64, 6);
            self.b.write_bits(v_delta >> self.trailing, sigbits as i32);

        }
    }
}

// bitRange returns whether the given integer can be represented by nbits.
fn bit_range(x:i64,nbits:u8) -> bool {
    return -((1<<(nbits-1))-1) <= x && x <= 1<<(nbits-1)
}

#[derive(Debug)]
pub enum Error {
    IO(io::Error),
}
#[derive(Debug)]
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
            t:-1 << 63,
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
        if self.num_read == 1 {
            match self.br.read_uvarint() {
                Ok(t_delta) => {
                    self.t_delta = t_delta;
                },
                Err(err) => {
                    self.err = Some(Error::IO(err));
                    return None;
                }
            }
            self.t = self.t + self.t_delta as i64;
            return self.read_value();
        }

        // read rest data point
        let mut d:u8 = 0;
        for i in 0..4 {
            d = d << 1;
            let bit = self.read_bit().ok()?;
            if bit == 0 {
                break
            }
            d = d | 1;
        }
        let mut sz:u8 = 0;
        let mut dod:i64 = 0;
        match d {
            0b0 => (),// dod == 0;
            0b10 =>{
                sz = 14;
            },
            0b110 => {
                sz = 17;
            },
            0b1110 => {
                sz = 20;
            },
            0b1111 => {
                // Do not use fast because it's very unlikely it will succeed.
                let bits = self.br.read_bits(64).ok()?;
                dod = bits as i64;
            },
            _ => {},
        }

        if sz != 0 {
            let mut bits = self.read_bits_or_fast(sz).ok()?;
            if bits > (1 << (sz -1)) {
                //bits = bits - (1 << sz);
                bits = bits.wrapping_sub(1<<sz);
            }
            dod = bits as i64;
        }
        self.t_delta = (self.t_delta as i64+ dod) as u64;
        self.t = self.t + (self.t_delta as i64);
        return  self.read_value();
    }
}

impl<'a> XORIterator<'a> {
    fn read_bit_or_fast(&mut self) -> Result<u8, io::Error> {
        match self.br.read_bit_fast() {
            Ok(b) => Ok(b as u8),
            Err(_) => self.br.read_bit().map(|b| b as u8)
        }
    }

    fn read_bits_or_fast(&mut self, n: u8) -> Result<u64, io::Error> {
        match self.br.read_bits_fast(n) {
            Ok(b) => Ok(b),
            Err(_) => self.br.read_bits(n)
        }
    }

    pub fn read_value(&mut self) -> Option<()>{
        let bit = self.read_bit_or_fast().ok()?;
        if bit == 0 {
            // do nothing
        }else {
            let bit = self.read_bit_or_fast().ok()?;
            if bit == 0 {
                // reuse leading/trailing zero bits
                // do nothing
            } else {
                let bits = self.read_bits_or_fast(5).ok()? as u8;
                self.leading = bits;
            
                let mut mbits = self.read_bits_or_fast(6).ok()? as u8;
                if mbits == 0 {
                    mbits = 64;
                }
                self.trailing = 64 - self.leading - mbits;
            }
            let mbits = 64 - self.leading - self.trailing;
            let bits = self.read_bits_or_fast(mbits).ok()?;
            
            let mut vbits = f64::to_bits(self.val);
            vbits = vbits ^ (bits << self.trailing);
            self.val = f64::from_bits(vbits);
        }
        self.num_read +=1;
        return Some(());
    }
}



#[test]
fn test_xor_chunk() {
    let mut chunk = XORChunk::new();
    let mut appender = chunk.appender().unwrap();

    #[derive(Debug,PartialEq)]
    struct DataPoint {
        ts:i64,
        val:f64,
    }

    let mut cases = vec![];
    let mut ts = 1234123324 as i64;
    let mut val = 1243535.123;
    for i in 0..300 {
        ts = ts + rand::thread_rng().gen_range(1..10001);
        if i % 2 == 0 {
            val = val + rand::thread_rng().gen_range(1..1000000) as f64;
        } else {
            val = val - rand::thread_rng().gen_range(1..1000000) as f64;
        }

        if i %10 == 0 {
            appender = chunk.appender().unwrap();
        }
        appender.Append(ts, val);
        cases.push(DataPoint{
            ts,
            val
        });
    }

    // 1. 
    let mut reader = chunk.iterator();
    let mut res = vec![];
    while let Some(_) = reader.next() {
        res.push(DataPoint{
            ts: reader.t,
            val: reader.val
        });
    }
    assert_eq!(res, cases);
}