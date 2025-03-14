use std::io;



struct Bstream {
    // todo: 写入的话，需要可变引用？读取则使用不可变引用
    stream : Vec<u8>, // data stream
    count: u8 // how many right-most bits are available for writing in the current byte
}

type Bit = bool;

const BIT_ONE: Bit = true;
const BIT_ZERO: Bit = false;

impl Bstream {
    // new
    // fn new() -> bstream {
    //     bstream {
    //         stream: Vec::new(),
    //         count: 0
    //     }
    // }
    fn bytes(&self) -> &Vec<u8> {
        &self.stream
    }

    fn write_bit(&mut self,bit: Bit) {
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

    fn write_byte(&mut self,byte: u8) {
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
    fn write_bits(&mut self,mut u:u64, mut nbits:i32) {
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
}


struct BstreamReader<'a> {
    stream : &'a Vec<u8>,
    stream_offset: usize,
    
    buffer : u64,
    valid: u8,
}

// enum BstreamError {

// }
impl<'a> BstreamReader<'a>{
    fn new(stream: &'a Vec<u8>) -> BstreamReader<'a> {
        BstreamReader {
            stream,
            stream_offset: 0,
            buffer: 0,
            valid: 0,
        }
    }

    fn read_bit(&mut self) -> Result<Bit,io::Error> {
        if self.valid == 0 {
            if !self.load_next_buffer(1){
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof))
            }
        }
        return self.read_bit_fast()
    }

    fn read_bit_fast(&mut self) -> Result<Bit,io::Error> {
        if self.valid == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof)) // todo: maybe use a custom error
        }
        self.valid -=1;
        let bitmask = 1 << self.valid;
        let bit = (self.buffer & bitmask)!= 0;
        Ok(bit)
    }
    
    fn read_bits(&mut self,mut nbits:u8) -> Result<u64,io::Error> {
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

    fn read_bits_fast(&mut self,nbits:u8) -> Result<u64,io::Error> {
        if nbits > self.valid { 
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof)) 
        }
        let bitmask = (1 << nbits) -1;
        self.valid -=nbits;
        Ok((self.buffer >> self.valid) & bitmask)

    }

    fn read_byte(&mut self) -> Result<u8,io::Error> {
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