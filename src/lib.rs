use csv::{Reader, Writer};
use serde::{Deserialize, Serialize};
use std::io::Result;

pub struct Database {
    path: String,
    extension: String,
}

impl Database {
    pub fn new(path: &str, extension: Option<&str>) -> Self {
        Self {
            path: String::from(path),
            extension: String::from(extension.unwrap_or("csv")),
        }
    }

    pub fn find<T, P>(&self, collection: &str, predicate: P) -> Result<Vec<T>>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        P: FnMut(&T) -> bool,
    {
        let mut rdr =
            Reader::from_path(format!("{}/{}.{}", self.path, collection, self.extension))?;
        let results: std::result::Result<Vec<T>, csv::Error> = rdr.deserialize().collect();

        Ok(results?.into_iter().filter(predicate).collect())
    }

    pub fn insert<T>(&self, collection: &str, document: T) -> Result<()>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let documents: Vec<T> = self.find(collection, |_| true)?;
        let mut wrt =
            Writer::from_path(format!("{}/{}.{}", self.path, collection, self.extension))?;

        for document in documents {
            wrt.serialize(document)?;
        }

        wrt.serialize(document)?;

        Ok(())
    }

    pub fn delete<T, P>(&self, collection: &str, predicate: P) -> Result<()>
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq,
        P: FnMut(&&T) -> bool,
    {
        let documents: Vec<T> = self.find(collection, |_| true)?;
        let remove: Vec<&T> = documents.iter().filter(predicate).collect();
        let results: Vec<&T> = documents.iter().filter(|d| !remove.contains(d)).collect();
        let mut wrt =
            Writer::from_path(format!("{}/{}.{}", self.path, collection, self.extension))?;

        for document in results {
            wrt.serialize(document)?;
        }

        Ok(())
    }

    pub fn update<T, P>(&self, collection: &str, document: T, predicate: P) -> Result<()>
    where
        T: Serialize + for<'de> serde::Deserialize<'de> + PartialEq,
        P: FnMut(&&T) -> bool,
    {
        let documents: Vec<T> = self.find(collection, |_| true)?;
        let remove: Vec<&T> = documents.iter().filter(predicate).collect();
        let results: Vec<&T> = documents.iter().filter(|d| !remove.contains(d)).collect();
        let mut wrt =
            Writer::from_path(format!("{}/{}.{}", self.path, collection, self.extension))?;

        for document in results {
            wrt.serialize(document)?;
        }

        wrt.serialize(document)?;

        Ok(())
    }
}
