use csv::{Reader, Writer};
use serde::{Deserialize, Serialize};
use std::{error::Error, path::Path};

pub struct Database<PA: AsRef<Path>> {
    path: PA,
    extension: String,
}

impl<PA: AsRef<Path>> Database<PA> {
    pub fn new(path: PA, extension: Option<&str>) -> Self {
        Self {
            path,
            extension: String::from(extension.unwrap_or("csv")),
        }
    }

    pub fn find<T, P>(&self, collection: &str, predicate: P) -> Result<Vec<T>, Box<dyn Error>>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        P: FnMut(&T) -> bool,
    {
        let mut rdr = Reader::from_path(
            self.path
                .as_ref()
                .join(format!("{}.{}", collection, self.extension)),
        )?;
        let results: Result<Vec<T>, csv::Error> = rdr.deserialize().collect();

        Ok(results?.into_iter().filter(predicate).collect())
    }

    pub fn insert<T>(&self, collection: &str, document: T) -> Result<(), Box<dyn Error>>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let documents: Vec<T> = self.find(collection, |_| true)?;
        let mut wrt = Writer::from_path(
            self.path
                .as_ref()
                .join(format!("{}.{}", collection, self.extension)),
        )?;

        for document in documents {
            wrt.serialize(document)?;
        }

        wrt.serialize(document)?;

        Ok(())
    }

    pub fn delete<T, P>(&self, collection: &str, predicate: P) -> Result<(), Box<dyn Error>>
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq,
        P: FnMut(&&T) -> bool,
    {
        let documents: Vec<T> = self.find(collection, |_| true)?;
        let remove: Vec<&T> = documents.iter().filter(predicate).collect();
        let results: Vec<&T> = documents.iter().filter(|d| !remove.contains(d)).collect();
        let mut wrt = Writer::from_path(
            self.path
                .as_ref()
                .join(format!("{}.{}", collection, self.extension)),
        )?;

        for document in results {
            wrt.serialize(document)?;
        }

        Ok(())
    }

    pub fn update<T, P>(
        &self,
        collection: &str,
        document: T,
        predicate: P,
    ) -> Result<(), Box<dyn Error>>
    where
        T: Serialize + for<'de> serde::Deserialize<'de> + PartialEq,
        P: FnMut(&&T) -> bool,
    {
        let documents: Vec<T> = self.find(collection, |_| true)?;
        let remove: Vec<&T> = documents.iter().filter(predicate).collect();
        let results: Vec<&T> = documents.iter().filter(|d| !remove.contains(d)).collect();
        let mut wrt = Writer::from_path(
            self.path
                .as_ref()
                .join(format!("{}.{}", collection, self.extension)),
        )?;

        for document in results {
            wrt.serialize(document)?;
        }

        wrt.serialize(document)?;

        Ok(())
    }
}
