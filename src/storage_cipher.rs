//! Internal authenticated envelopes shared by publication snapshots and journals.
use crate::{Error, Result};
use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use rand::{RngCore, rngs::OsRng};
use zeroize::{Zeroize, Zeroizing};

pub(crate) struct Cipher(Zeroizing<[u8; 32]>);
impl Cipher {
    pub(crate) fn new(mut key: [u8; 32]) -> Self {
        let cipher = Self(Zeroizing::new(key));
        key.zeroize();
        cipher
    }
    pub(crate) fn same_key(&self, key: &[u8; 32]) -> bool {
        self.0
            .iter()
            .zip(key)
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            == 0
    }
    pub(crate) fn seal(&self, plain: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(self.0.as_ref()).map_err(|_| integrity())?;
        let mut nonce = [0; 12];
        OsRng.fill_bytes(&mut nonce);
        let mut result = nonce.to_vec();
        result.extend(
            cipher
                .encrypt(Nonce::from_slice(&nonce), Payload { msg: plain, aad })
                .map_err(|_| integrity())?,
        );
        Ok(result)
    }
    pub(crate) fn open(&self, sealed: &[u8], aad: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        if sealed.len() < 28 {
            return Err(integrity());
        }
        let cipher = Aes256Gcm::new_from_slice(self.0.as_ref()).map_err(|_| integrity())?;
        Ok(Zeroizing::new(
            cipher
                .decrypt(
                    Nonce::from_slice(&sealed[..12]),
                    Payload {
                        msg: &sealed[12..],
                        aad,
                    },
                )
                .map_err(|_| integrity())?,
        ))
    }
}
fn integrity() -> Error {
    Error::Unknown("encrypted storage key or integrity mismatch; preserve original".into())
}
