# Configuration file for the Sphinx documentation builder.


# -- Path setup --------------------------------------------------------------
import datetime
import os
import sys

sys.path.insert(0, os.path.abspath(".."))

# -- Project information -----------------------------------------------------

project = "rdf4j-python"
copyright = f"{datetime.date.today().year}, Chengxu Bian"
author = "Chengxu Bian"
version = "0.1.6"
release = "0.1.6"

# -- General configuration ---------------------------------------------------

extensions = [
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

html_theme = "furo"
html_context = {
    "display_github": True,
    "github_user": "odysa",
    "github_repo": "rdf4j-python",
    "github_version": "develop",
    "conf_py_path": "/docs/",
}


exclude_patterns = ["dist", ".DS_Store", ".venv", ".pytest_cache"]

master_doc = "index"
