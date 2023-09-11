# CSV DB

A simple embedded NoSQL database using CSV files for storage.

It allows using CSV files to perform these operations on collections of documents:

* find
* insert
* delete
* update

# Design

`csv_db` wraps a thin layer around the [`csv`] crate, providing a Database struct with the usual
methods expected from a database library. Its main purpose is to abstract the user further away
from the low level details of CSV files, namely dealing with opening files and working with
records. Additionally the crate tries to be as generic as possible, allowing the user to
seamlessly use any custom type as document. In order to achieve this easily, `csv_db`'s methods
expect generic documents to implement `Serialize`/`Deserialize` traits using [`serde`]'s derive
macros.

The methods are all asynchronous, so that you can easily integrate this crate into asynchronous
code. To achieve this, and since the [`csv`] crate isn't asynchronous, each method of `csv_db`
wraps any potentially blocking code (like opening files or dealing with records) inside a
[`tokio::task::spawn_blocking`] function.

[`csv`]: https://docs.rs/csv/1.2.2/x86_64-unknown-linux-gnu/csv/index.html
[`serde`]: https://docs.rs/serde/1.0.188/x86_64-unknown-linux-gnu/serde/index.html
[`tokio::task::spawn_blocking`]: https://docs.rs/tokio/1.32.0/x86_64-unknown-linux-gnu/tokio/task/blocking/fn.spawn_blocking.html

# Examples

```rust
use csv_db::Database;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct User {
    id: usize,
    first_name: String,
    last_name: String,
    age: u32,
}

#[tokio::main]
async fn main() {
    // Create a new Database with a mandatory path and an optional file extension.
    let db = Database::new("data", None);

    let user = User {
        id: 1,
        first_name: String::from("First"),
        last_name: String::from("Last"),
        age: 20,
    };

    // Insert a new user into the users collection.
    db.insert("users", user)
        .await
        .expect("Problem inserting user.");

    // Find users by filtering with a predicate on the users collection.
    let adults = db
        .find("users", |u: &User| u.age >= 18)
        .await
        .expect("Problem searching user.");

    let user = User {
        id: 1,
        first_name: String::from("First"),
        last_name: String::from("Last"),
        age: 21,
    };

    // Update a user by filtering with a predicate on the users collection.
    db.update("users", user, |u: &&User| u.id == 1)
        .await
        .expect("Problem updating user.");

    // Delete a user by filtering with a predicate from the users collection.
    db.delete("users", |u: &&User| u.id == 1)
        .await
        .expect("Problem deleting user.");
}
```