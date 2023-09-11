use csv::{Reader, Writer};
use serde::{Deserialize, Serialize};
use std::{error::Error, path::Path, sync::Arc};
use tokio::{task, task::JoinError};

pub struct Config<PA> {
    path: PA,
    extension: String,
}
pub struct Database<PA> {
    config: Arc<Config<PA>>,
}

impl<PA> Database<PA>
where
    PA: AsRef<Path> + Send + Sync + Clone + 'static,
{
    pub fn new(path: PA, extension: Option<&str>) -> Self {
        Self {
            config: Arc::new(Config {
                path,
                extension: String::from(extension.unwrap_or("csv")),
            }),
        }
    }

    pub async fn find<T, P>(&self, collection: &str, predicate: P) -> Result<Vec<T>, Box<dyn Error>>
    where
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        P: FnMut(&T) -> bool,
    {
        let collection = collection.to_string();
        let config = self.config.clone();
        let results: Result<Result<Vec<T>, _>, _> = task::spawn_blocking(move || {
            let mut rdr = Reader::from_path(
                config
                    .path
                    .as_ref()
                    .join(format!("{}.{}", collection, config.extension)),
            )?;

            rdr.deserialize().collect()
        })
        .await;

        Ok(results??.into_iter().filter(predicate).collect())
    }

    pub async fn insert<T>(&self, collection: &str, document: T) -> Result<(), Box<dyn Error>>
    where
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        let documents: Vec<T> = self.clone().find(collection, |_| true).await?;
        let collection = collection.to_string();
        let config = self.config.clone();
        let result: Result<Result<(), csv::Error>, JoinError> = task::spawn_blocking(move || {
            let mut wrt = Writer::from_path(
                config
                    .path
                    .as_ref()
                    .join(format!("{}.{}", collection, config.extension)),
            )?;

            for document in documents {
                wrt.serialize(document)?;
            }

            wrt.serialize(document)?;

            Ok(())
        })
        .await;

        Ok(result??)
    }

    pub async fn delete<T, P>(
        &self,
        collection: &str,
        mut predicate: P,
    ) -> Result<(), Box<dyn Error>>
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq + Send + 'static,
        P: FnMut(&&T) -> bool,
    {
        let mut documents: Vec<T> = self.clone().find(collection, |_| true).await?;
        let collection = collection.to_string();
        let config = self.config.clone();

        documents.retain(|d| !predicate(&d));

        let result: Result<Result<(), csv::Error>, JoinError> = task::spawn_blocking(move || {
            let mut wrt = Writer::from_path(
                config
                    .path
                    .as_ref()
                    .join(format!("{}.{}", collection, config.extension)),
            )?;

            for document in &documents {
                wrt.serialize(document)?;
            }

            Ok(())
        })
        .await;

        Ok(result??)
    }

    pub async fn update<T, P>(
        &self,
        collection: &str,
        document: T,
        mut predicate: P,
    ) -> Result<(), Box<dyn Error>>
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq + Send + 'static,
        P: FnMut(&&T) -> bool,
    {
        let mut documents: Vec<T> = self.clone().find(collection, |_| true).await?;
        let collection = collection.to_string();
        let config = self.config.clone();

        documents.retain(|d| !predicate(&d));

        let result: Result<Result<(), csv::Error>, JoinError> = task::spawn_blocking(move || {
            let mut wrt = Writer::from_path(
                config
                    .path
                    .as_ref()
                    .join(format!("{}.{}", collection, config.extension)),
            )?;

            for document in documents {
                wrt.serialize(document)?;
            }

            wrt.serialize(document)?;

            Ok(())
        })
        .await;

        Ok(result??)
    }
}
