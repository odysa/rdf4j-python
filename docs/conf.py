# Configuration file for the Sphinx documentation builder.


# -- Path setup --------------------------------------------------------------
import os
import sys

sys.path.insert(0, os.path.abspath(".."))

import datetime

from rdf4j_python import __authors__, __version__

# -- Project information -----------------------------------------------------

project = "rdf4j-python"
copyright = f"{datetime.date.today().year}, {__authors__[0]['name']}"
author = __authors__[0]["name"]
version = __version__
release = __version__

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.apidoc",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",  # for Google/NumPy-style docstrings
    "sphinx.ext.viewcode",
]

autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
    "inherited-members": True,
    "no-special-members": True,
}

source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}

templates_path = ["_templates"]

# -- Options for HTML output -------------------------------------------------

html_theme = "sphinx_rtd_theme"
html_context = {
    "display_github": True,
    "github_user": "odysa",
    "github_repo": "rdf4j-python",
    "github_version": "develop",
    "conf_py_path": "/docs/",
}


exclude_patterns = ["dist", ".DS_Store", ".venv", ".pytest_cache"]

master_doc = "index"
