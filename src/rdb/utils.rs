use tokio::io::{AsyncRead, AsyncReadExt};

pub(crate) fn int_to_vec(number: i32) -> Vec<u8> {
    let number = number.to_string();
    let mut result = Vec::with_capacity(number.len());
    for &c in number.as_bytes().iter() {
        result.push(c);
    }
    result
}

pub(crate) async fn read_exact<T: AsyncRead + Unpin>(
    reader: &mut T,
    len: usize,
) -> crate::Result<Vec<u8>> {
    let mut buf = vec![0; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}