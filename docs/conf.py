# Configuration file for the Sphinx documentation builder.

import os
import sys

# -- Path setup --------------------------------------------------------------

# If your module is in the parent directory, add it to sys.path
sys.path.insert(0, os.path.abspath(".."))

# -- Project information -----------------------------------------------------

project = "rdf4j-python"
copyright = "2025, Chengxu Bian"
author = "Chengxu Bian"
release = "0.1.1a"

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",  # for Google/NumPy-style docstrings
    "sphinx.ext.viewcode",
    "sphinx_automodapi",
    "myst_parser",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}

templates_path = ["_templates"]
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

html_theme = "alabaster"  # Or 'sphinx_rtd_theme', 'alabaster', etc.
html_context = {
    "display_github": True,
    "github_user": "odysa",
    "github_repo": "rdf4j-python",
    "github_version": "develop",
    "conf_py_path": "/docs/",
}


exclude_patterns = ["dist", ".DS_Store", ".venv", ".pytest_cache"]

master_doc = "index"
