# 🐍 rdf4j-python

**A Pythonic interface to the powerful Java-based [Eclipse RDF4J](https://rdf4j.org/) framework.**

> ⚠️ **Note:** This project is currently under active development and considered **experimental**. Interfaces may change. Use with caution in production environments—and feel free to help shape its future!

✅ **Supports both asynchronous (`async/await`) and synchronous programming styles.**

## 🌐 Overview

`rdf4j-python` bridges the gap between Python applications and the [Eclipse RDF4J](https://rdf4j.org/) framework, enabling seamless interaction with RDF4J repositories directly from Python. This integration allows developers to leverage RDF4J's robust capabilities for managing RDF data and executing SPARQL queries without leaving the Python ecosystem.

## 🚀 Features

- **Seamless Integration**: Interact with RDF4J repositories using Pythonic constructs.
- **SPARQL Support**: Execute SPARQL queries and updates effortlessly.
- **Repository Management**: Create, access, and manage RDF4J repositories programmatically.
- **Data Handling**: Add and retrieve RDF triples with ease.
- **Format Flexibility**: Support for various RDF serialization formats.

## 📦 Installation

Install via pip:

```bash
pip install rdf4j-python
```

## 🧪 Usage (Async)

Here's a basic example of how to use `rdf4j-python` to create an in-memory sail repository

```python
from rdf4j_python import AsyncRdf4j
from rdf4j_python.model import MemoryStoreConfig, RepositoryConfig
from rdf4j_python.utils.const import Rdf4jContentType

async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
    repo_config = (
        RepositoryConfig.builder_with_sail_repository(
            MemoryStoreConfig.Builder().persist(False).build(),
        )
        .repo_id("example-repo")
        .title("Example Repository")
        .build()
    )
    await db.create_repository(
        repository_id=repo_config.repo_id,
        rdf_config_data=repo_config.to_turtle(),
        content_type=Rdf4jContentType.TURTLE,
    )
```

For more detailed examples, refer to the [examples](https://github.com/odysa/rdf4j-python/tree/main/examples) directory.

## 🤝 Contributing

We welcome contributions and feedback! If you'd like to help improve this project:

- Fork the repo and submit a pull request
- Open an issue for bugs, feature ideas, or discussions
- ⭐ Star the repo if you find it useful!

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/odysa/rdf4j-python/blob/main/LICENSE) file for details.
