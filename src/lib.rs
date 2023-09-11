//! # CSV DB
//!
//! A simple embedded NoSQL database using CSV files for storage.
//!
//! It allows using CSV files to perform these operations on collections of documents:
//!
//! * find
//! * insert
//! * delete
//! * update
//!
//! # Design
//!
//! `csv_db` wraps a thin layer around the [`csv`] crate, providing a Database struct with the usual
//! methods expected from a database library. Its main purpose is to abstract the user further away
//! from the low level details of CSV files, namely dealing with opening files and working with
//! records. Additionally the crate tries to be as generic as possible, allowing the user to
//! seamlessly use any custom type as document. In order to achieve this easily, `csv_db`'s methods
//! expect generic documents to implement `Serialize`/`Deserialize` traits using [`serde`]'s derive
//! macros.
//!
//! The methods are all asynchronous, so that you can easily integrate this crate into asynchronous
//! code. To achieve this, and since the [`csv`] isn't asynchronous, each method of `csv_db` wraps
//! any potentially blocking code (like opening files or dealing with records) inside a
//! [`tokio::task::spawn_blocking`] function.
//!
//! # Examples
//!
//! ```
//! use csv_db::Database;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Deserialize, PartialEq, Serialize)]
//! struct User {
//!     id: usize,
//!     first_name: String,
//!     last_name: String,
//!     age: u32,
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create a new Database with a mandatory path and an optional file extension.
//!     let db = Database::new("data", None);
//!
//!     let user = User {
//!         id: 1,
//!         first_name: String::from("First"),
//!         last_name: String::from("Last"),
//!         age: 20,
//!     };
//!
//!     // Insert a new user into the users collection.
//!     db.insert("users", user)
//!         .await
//!         .expect("Problem inserting user.");
//!
//!     // Find users by filtering with a predicate on the users collection.
//!     let adults = db
//!         .find("users", |u: &User| u.age >= 18)
//!         .await
//!         .expect("Problem searching user.");
//!
//!     let user = User {
//!         id: 1,
//!         first_name: String::from("First"),
//!         last_name: String::from("Last"),
//!         age: 21,
//!     };
//!
//!     // Update a user by filtering with a predicate on the users collection.
//!     db.update("users", user, |u: &&User| u.id == 1)
//!         .await
//!         .expect("Problem updating user.");
//!
//!     // Delete a user by filtering with a predicate from the users collection.
//!     db.delete("users", |u: &&User| u.id == 1)
//!         .await
//!         .expect("Problem deleting user.");
//! }
//! ```

use csv::{Reader, Writer};
use serde::{Deserialize, Serialize};
use std::{error::Error, path::Path, sync::Arc};
use tokio::{task, task::JoinError};

struct Config<PA> {
    path: PA,
    extension: String,
}

/// A Database provides methods to access data.
///
/// The config private field is wrapped in an Arc to be shared among threads.
pub struct Database<PA> {
    config: Arc<Config<PA>>,
}

impl<PA> Database<PA>
where
    PA: AsRef<Path> + Send + Sync + Clone + 'static,
{
    /// Create a new Database with a mandatory path and an optional file extension.
    ///
    /// # Examples
    ///
    /// ```
    /// use csv_db::Database;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Deserialize, PartialEq, Serialize)]
    /// struct User {
    ///     id: usize,
    ///     first_name: String,
    ///     last_name: String,
    ///     age: u32,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let db = Database::new("data", None);
    /// }
    /// ```
    pub fn new(path: PA, extension: Option<&str>) -> Self {
        Self {
            config: Arc::new(Config {
                path,
                extension: String::from(extension.unwrap_or("csv")),
            }),
        }
    }

    /// Find documents by filtering with a predicate on a collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use csv_db::Database;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Deserialize, PartialEq, Serialize)]
    /// struct User {
    ///     id: usize,
    ///     first_name: String,
    ///     last_name: String,
    ///     age: u32,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let db = Database::new("data", None);
    ///
    ///     let user = User {
    ///         id: 1,
    ///         first_name: String::from("First"),
    ///         last_name: String::from("Last"),
    ///         age: 20,
    ///     };
    ///
    ///     let adults = db
    ///         .find("users", |u: &User| u.age >= 18)
    ///         .await
    ///         .expect("Problem searching user.");
    /// }
    /// ```
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

    /// Insert a new document into a collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use csv_db::Database;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Deserialize, PartialEq, Serialize)]
    /// struct User {
    ///     id: usize,
    ///     first_name: String,
    ///     last_name: String,
    ///     age: u32,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let db = Database::new("data", None);
    ///
    ///     let user = User {
    ///         id: 1,
    ///         first_name: String::from("First"),
    ///         last_name: String::from("Last"),
    ///         age: 20,
    ///     };
    ///
    ///     db.insert("users", user)
    ///         .await
    ///         .expect("Problem inserting user.");
    /// }
    /// ```
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

    /// Delete a document by filtering with a predicate from a collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use csv_db::Database;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Deserialize, PartialEq, Serialize)]
    /// struct User {
    ///     id: usize,
    ///     first_name: String,
    ///     last_name: String,
    ///     age: u32,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let db = Database::new("data", None);
    ///
    ///     db.delete("users", |u: &&User| u.id == 1)
    ///         .await
    ///         .expect("Problem deleting user.");
    /// }
    /// ```
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

    /// Update a document by filtering with a predicate on a collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use csv_db::Database;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Deserialize, PartialEq, Serialize)]
    /// struct User {
    ///     id: usize,
    ///     first_name: String,
    ///     last_name: String,
    ///     age: u32,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let db = Database::new("data", None);
    ///
    ///     let user = User {
    ///         id: 1,
    ///         first_name: String::from("First"),
    ///         last_name: String::from("Last"),
    ///         age: 21,
    ///     };
    ///
    ///     db.update("users", user, |u: &&User| u.id == 1)
    ///         .await
    ///         .expect("Problem updating user.");
    /// }
    /// ```
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
