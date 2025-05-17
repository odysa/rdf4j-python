# Configuration file for the Sphinx documentation builder.

import datetime
import os
import sys

import rdf4j_python

# -- Path setup --------------------------------------------------------------

# If your module is in the parent directory, add it to sys.path
sys.path.insert(0, os.path.abspath(".."))

# -- Project information -----------------------------------------------------

project = "rdf4j-python"
copyright = f"{datetime.date.today().year}, {rdf4j_python.__author__}"
author = rdf4j_python.__author__
version = rdf4j_python.__version__
release = rdf4j_python.__version__

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.apidoc",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",  # for Google/NumPy-style docstrings
    "sphinx.ext.viewcode",
    "sphinx.ext.autosummary",
]

autosummary_generate = True

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
