use rand::distributions::Alphanumeric;
use rand::Rng;

pub fn generate_id(len: Option<usize>) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .filter_map(|b| {
            let c = b as char;
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                Some(c)
            } else {
                None
            }
        })
        .take(len.unwrap_or(16))
        .collect()
}