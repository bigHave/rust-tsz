use std::io;


// Bstream is a stream of bits
pub struct Bstream {
    stream : Vec<u8>, // data stream
    count: u8 // how many right-most bits are available for writing in the current byte
}

type Bit = bool;

const BIT_ONE: Bit = true;
const BIT_ZERO: Bit = false;
const MAX_VARINT_LEN64: usize = 10;

impl Bstream {
    // new
    pub fn new(stream:Vec<u8>) -> Bstream {
        Bstream {
            stream,
            count: 0
        }
    }
    
    // this is some particial method for xor chunk to update the chunk header
    // maybe this is not a good way to do this
    pub fn modify_first_two_bytes(&mut self,byt1:u8,byt2:u8) {
        if self.stream.len() > 2 {
            self.stream[0] = byt1;
            self.stream[1] = byt2;
        }else {
            self.stream.resize(2, 0);
            self.stream[0] = byt1;
            self.stream[1] = byt2;
        }
    }

    pub fn bytes(&mut self) -> &Vec<u8> {
        &self.stream
    }

    pub fn write_bit(&mut self,bit: Bit) {
        if self.count == 0 {
            self.stream.push(0);
            self.count = 8;
        }

        let i = self.stream.len()-1;
        // if write one, 
        // if write zero, do nothing but increase count
        if bit{
            self.stream[i] |= 1 <<(self.count -1);
        }
        self.count -= 1;
    }

    pub fn write_byte(&mut self,byte: u8) {
        if self.count == 0 {
            self.stream.push(0);
            self.count = 8;
        }

        let mut i = self.stream.len()-1;
        // write the left-most (8-self.count) bits to the last byte in stream
        self.stream[i] |= byte >> (8-self.count);
        self.stream.push(0);
        i +=1;
        // write the remainder
        self.stream[i] = byte.checked_shl(self.count as u32).unwrap_or(0); // note: maybe there is a better way to do this?
    }

    // write_bits writes the nbits right-most bits of u to the stream in left-to-right order.
    pub fn write_bits(&mut self,mut u:u64, mut nbits:i32) {
        //let mut nbits = nbits as u8;
        u = u << (64 - nbits);
        while nbits >= 8 {
            let byt = (u >> 56) as u8;
            self.write_byte(byt);
            u = u << 8;
            nbits -= 8;
            
        }
        // for byte in self.bytes() {
        //     println!("{:08b}",byte);
        // }
        while nbits > 0 {
            //println!("{:08b}",u);
            let bit = u >> 63;
            self.write_bit(bit == 1);
            u = u << 1;
            nbits -=1;
        }
    }

    pub fn write_uvarint(&mut self,mut u:u64) {
        while u >= 0x80 {
            let byte = u as u8 | 0x80;
            self.write_byte(byte);
            u = u >> 7;
        }
        self.write_byte(u as u8)
    }

    pub fn write_varint(&mut self,i:i64) {
        let mut ui = (i as u64) << 1;
        if i < 0 {
            ui = !ui;
        }
        self.write_uvarint(ui)
    }
}

#[derive(Debug)]
pub struct BstreamReader<'a> {
    stream : &'a Vec<u8>,
    stream_offset: usize,
    
    buffer : u64,
    valid: u8,
}

// enum BstreamError {

// }
impl<'a> BstreamReader<'a>{
    pub fn new(stream: &'a Vec<u8>) -> BstreamReader<'a> {
        BstreamReader {
            stream,
            stream_offset:0,
            buffer: 0,
            valid: 0,
        }
    }

    pub fn read_bit(&mut self) -> Result<Bit,io::Error> {
        if self.valid == 0 {
            if !self.load_next_buffer(1){
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof))
            }
        }
        return self.read_bit_fast()
    }

    pub fn read_bit_fast(&mut self) -> Result<Bit,io::Error> {
        if self.valid == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof)) // todo: maybe use a custom error
        }
        self.valid -=1;
        let bitmask = 1 << self.valid;
        let bit = (self.buffer & bitmask)!= 0;
        Ok(bit)
    }
    
    pub fn read_bits(&mut self,mut nbits:u8) -> Result<u64,io::Error> {
        if self.valid == 0 {
            if !self.load_next_buffer(nbits) {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof))
            }
        }
        if nbits <= self.valid {
            return self.read_bits_fast(nbits)
        }
        // We have to read all remaining valid bits from the current buffer and a part from the next one.
        let mut bitmask = (1 << self.valid) -1;
        nbits -= self.valid;
        let mut v = (self.buffer & bitmask) << nbits;
        self.valid = 0;

        if !self.load_next_buffer(nbits) {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof))
        }
        bitmask = (1 << nbits) -1;
        v = v | ((self.buffer >> (self.valid - nbits)) & bitmask);
        self.valid -=nbits;
        Ok(v)
    }

    pub fn read_bits_fast(&mut self,nbits:u8) -> Result<u64,io::Error> {
        if nbits > self.valid { 
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof)) 
        }
        let bitmask = (1 << nbits) -1;
        self.valid -=nbits;
        Ok((self.buffer >> self.valid) & bitmask)

    }

    pub fn read_byte(&mut self) -> Result<u8,io::Error> {
        match self.read_bits(8){
            Ok(bits) => Ok(bits as u8),
            Err(e) => Err(e)
        }
    }

    // loadNextBuffer loads the next bytes from the stream into the internal buffer.
    // The input nbits is the minimum number of bits that must be read, but the implementation
    // can read more (if possible) to improve performances.
    fn load_next_buffer(&mut self,nbits:u8) -> bool {
        if self.stream_offset >= self.stream.len() {
            return false
        }

        if self.stream_offset+8 < self.stream.len() {
            //  self.stream[self.stream_offset..self.stream_offset+8]
            self.buffer = u64::from_be_bytes(self.stream[self.stream_offset..self.stream_offset+8].try_into().unwrap());
            self.stream_offset +=8;
            self.valid = 64;
            return true
        }
        let mut n_bytes = (nbits/8) as usize;
        n_bytes +=1;
        if self.stream_offset + n_bytes > self.stream.len() {
            n_bytes = self.stream.len() - self.stream_offset;
        }
        let mut buffer:u64 = 0;
        for i in 0..n_bytes {
            buffer = buffer | (u64::from(self.stream[self.stream_offset+i]) << (8*(n_bytes-i-1)));
        }
        self.buffer = buffer;
        self.stream_offset += n_bytes;
        self.valid = 8*n_bytes as u8;
        
        return true
    }


    pub fn read_uvarint(&mut self) -> Result<u64,io::Error> {
        let mut x:u64 = 0;
        let mut s:usize = 0;
        let mut i:usize = 0;
        loop {
            match self.read_byte(){
                // when error happens, no need to keep offset right
                Ok(byte) => {
                    if i == MAX_VARINT_LEN64 {
                        // overflow
                        return Ok(0)
                    }
                    if byte < 0x80 {
                        // overflow
                        if i == MAX_VARINT_LEN64-1 && byte > 1 {
                            return Ok(0)
                        }
                        return Ok(x | (u64::from(byte) << s));
                    }
                    i+=1;
                    x = x | (u64::from(byte & 0x7f) << s);
                    s += 7;
                }
                Err(e) => {
                    return Err(e)
                }
            }
        }
    }

    pub fn read_varint(&mut self) -> Result<i64,io::Error> {
        let ux = self.read_uvarint()?;
        let mut x = (ux >> 1) as i64;
        if ux & 1 != 0 {
            x = !x
        }
        return Ok(x);
    }
}




#[test]
fn test_bstream() {
    let mut bstream = Bstream {
        stream: vec![],
        count: 0
    };

    // test writing bit
    for bit in vec![BIT_ONE,BIT_ZERO,BIT_ONE,BIT_ZERO] {
        bstream.write_bit(bit);
    }
    
    // test writing byte
    bstream.write_byte(0b10101011);

    // test writing bits
    for nbits in 1..=64 {
        bstream.write_bits(nbits,nbits as i32)
    }

    for v in (1..10000).step_by(123) {
        bstream.write_bits(v,29)
    }
    
    // test reading bit
    let mut r = BstreamReader::new(bstream.bytes());
    for bit in vec![BIT_ONE,BIT_ZERO,BIT_ONE,BIT_ZERO] {
        let v: Bit;
        match r.read_bit_fast() {
            Ok(bit) => v = bit,
            Err(_) => {
                match r.read_bit() {
                    Ok(bit) => v = bit,
                    Err(e) => {
                        println!("{:?}",e);
                        panic!("read bit failed")
                    }
                }
            }
        }
        assert_eq!(bit,v);
    }

    // test reading byte
    let v = r.read_byte().unwrap();
    assert_eq!(0b10101011,v);

    // test reading bits
    for nbits in 1..=64 {
        let v: u64;
        match r.read_bits_fast(nbits) {
            Ok(bits) => v = bits,
            Err(_) => {
                match r.read_bits(nbits) {
                    Ok(bits) => v = bits,
                    Err(e) => {
                        println!("{:?}",e);
                        panic!("read bits failed")
                    }
                }
            }
        }
        assert_eq!(nbits as u64,v);
    }
    for v in (1..10000).step_by(123) {
        let actual: u64;
        match r.read_bits_fast(29) {
            Ok(bits) => actual = bits,
            Err(_) => {
                match r.read_bits(29) {
                    Ok(bits) => actual = bits,
                    Err(e) => {
                        println!("{:?}",e);
                        panic!("read bits failed")
                    }
                }
            }
        }
        assert_eq!(v as u64,actual);
    }
}

static cases : [i64;17] = [
    -1 << 63,
    // -1 << 63 +1,
    -1,
    0,
    1,
    2,
    10,
    20,
    63,
    64,
    65,
    127,
    128,
    129,
    255,
    256,
    257,
    1 << 63-1
];

#[test]
fn test_uvarint() {
    let mut bstream = Bstream {
        stream: vec![],
        count: 0
    };


    // write
    for v in &cases {
        bstream.write_uvarint(*v as u64);
    }
    let mut r = BstreamReader::new(bstream.bytes());
    for v in &cases {
        let actual = r.read_uvarint().unwrap();
        assert_eq!(*v as u64,actual);
    }
}


#[test]
fn test_varint() {
    let mut bstream = Bstream {
        stream: vec![],
        count: 0
    };


    // write
    for v in &cases {
        bstream.write_varint(*v);
    }
    let mut r = BstreamReader::new(bstream.bytes());
    for v in &cases {
        let actual = r.read_varint().unwrap();
        assert_eq!(*v,actual);
    }
}