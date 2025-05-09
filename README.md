# 🐍 rdf4j-python

**A Pythonic interface to the powerful Java-based RDF4J framework.**

## 🌐 Overview

`rdf4j-python` bridges the gap between Python applications and the [Eclipse RDF4J](https://rdf4j.org/) framework, enabling seamless interaction with RDF4J repositories directly from Python. This integration allows developers to leverage RDF4J's robust capabilities for managing RDF data and executing SPARQL queries without leaving the Python ecosystem.

## 🚀 Features

- **Seamless Integration**: Interact with RDF4J repositories using Pythonic constructs.
- **SPARQL Support**: Execute SPARQL queries and updates effortlessly.
- **Repository Management**: Create, access, and manage RDF4J repositories programmatically.
- **Data Handling**: Add and retrieve RDF triples with ease.
- **Format Flexibility**: Support for various RDF serialization formats.

## 📦 Installation

Install the package using pip:

```bash
pip install rdf4j-python
```

## 🧪 Usage

Here's a basic example of how to use `rdf4j-python`:

```python
from rdf4j_python import AsyncRDF4JDB

async with AsyncRDF4JDB() as db:
    await db.create_repository("my_repo")
```

For more detailed examples, refer to the [examples](https://github.com/odysa/rdf4j-python/tree/main/examples) directory.

## 🤝 Contributing

Contributions are welcome! If you'd like to contribute, please fork the repository and submit a pull request. For major changes, please open an issue first to discuss what you would like to change.

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/odysa/rdf4j-python/blob/main/LICENSE) file for details.
