# keylime

Keylime is a simple key-value store that I wrote as a learning experience in building a (very) simple database from scratch.
At a high level, the store consists of one or more named collections, each of which contains a group of documents. The documents
are indexed using a B-tree to minimize the number of I/O operations needed to find a document with a specific key.

Some other neat features include:

* Each document field has a data type, which can be inferred when creating a new document
* Collections can optionally be associated with a schema when created, thereby constraining the fields a document can have
* Comes with a basic query language

## Querying the database

The repo comes with a REPL. To launch it, run the following command from root:

```
make repl
```

Example:

```
# Create a collection without a schema
KL> CREATE someCollection;

# Create a collection with a schema
KL> WITH SCHEMA {
  name: String,       
  email: String(5,16),   # string between 5-16 characters long
  age: Number?           # Optional
  alive: Boolean = false # Default value
} CREATE users;

# Create a document with a JSON payload
KL> WITH '{
  "name": "Nam",
  "email": "someemail@email.com"
}' SET user1 IN users;

# Get a document from a collection
KL> GET user1 IN users;

# Or select a subset of a document's fields
KL> GET name, email FROM user1 IN users;

# Get the last 5 documents inserted into the collection
KL> LAST 5 IN users;
[
  {
    "Key": "user1",
    "Fields": {
      "age": {
        "Type": "Number",
        "Value": 4
      }
    },
    "CreatedAt": "2021-07-10T18:45:43.452380469+02:00",
    "LastModified": "2021-07-10T18:45:43.452381617+02:00",
    "Deleted": false
  },
  ...
]
```
