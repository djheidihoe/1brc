use std::fs::File;

fn main() {
    let path = "/Users/andreas/git/1brc/data/measurements.txt";
    match File::open(path) {
        Ok(_) => println!("File opened successfully"),
        Err(e) => println!("Error: {}", e),
    }
}
