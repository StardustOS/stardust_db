use std::{
    env::temp_dir,
    fs::remove_dir_all,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::Mutex,
};

use once_cell::sync::OnceCell;
use rand::{distributions::Alphanumeric, prelude::StdRng, Rng, SeedableRng};

use crate::error::Result;
use crate::Database;

pub struct TemporaryDatabase {
    db: Database,
    path: PathBuf,
}

impl TemporaryDatabase {
    pub fn new() -> Result<Self> {
        static RNG: OnceCell<Mutex<StdRng>> = OnceCell::new();
        let rng = RNG.get_or_init(|| Mutex::new(StdRng::seed_from_u64(0)));

        let path = loop {
            let random: String = rng
                .lock()
                .as_mut()
                .unwrap()
                .deref_mut()
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

    pub fn path(&self) -> &Path {
        &self.path
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
