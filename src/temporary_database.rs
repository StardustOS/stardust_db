use std::{
    env::temp_dir,
    fs::remove_dir_all,
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use rand::{distributions::Alphanumeric, thread_rng, Rng};

use crate::error::Result;
use crate::Database;

pub struct TemporaryDatabase {
    db: Database,
    path: PathBuf,
}

impl TemporaryDatabase {
    pub fn new() -> Result<Self> {
        let path = loop {
            let random: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(12)
                .map(char::from)
                .collect();
            let path = temp_dir().join(random);
            if !path.exists() {
                break path;
            }
        };
        let db = Database::open(&path)?;
        Ok(Self { db, path })
    }
}

impl Deref for TemporaryDatabase {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl DerefMut for TemporaryDatabase {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}

impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        remove_dir_all(&self.path).unwrap()
    }
}

pub fn temp_db() -> TemporaryDatabase {
    TemporaryDatabase::new().unwrap()
}
