fn parity_bit(bytes: &[u8]) -> u8 {
    let mut n_ones: u32 = 0;
   for byte in bytes {
      n_ones += byte.count_ones();
   }
   (n_ones % 2 == 0) as u8
}

fn main() {

   let abc = b"abc";
   println!("abc:");
   println!("input: {:?}", abc);
   println!("parity_bit: {:08x}", parity_bit(abc));

   println!("abcd:");
   let abcd = b"abcd";
   println!("input: {:?}", abcd);
   println!("parity_bit: {:08x}", parity_bit(abcd))
}