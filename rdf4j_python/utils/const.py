from enum import Enum


class Rdf4jContentType(str, Enum):
    # SPARQL Query Results
    SPARQL_RESULTS_JSON = "application/sparql-results+json"
    SPARQL_RESULTS_XML = "application/sparql-results+xml"
    SPARQL_RESULTS_BINARY = "application/x-binary-rdf-results-table"
    BOOLEAN_TEXT = "text/boolean"

    # RDF Serialization Formats
    RDF_XML = "application/rdf+xml"
    RDF_JSON = "application/rdf+json"
    LD_JSON = "application/ld+json"
    NTRIPLES = "text/plain"
    TURTLE = "text/turtle"
    N3 = "text/rdf+n3"
    NQUADS = "text/x-nquads"
    TRIG = "application/x-trig"
    TRIX = "application/trix"
    BINARY_RDF = "application/x-binary-rdf"

    # SPARQL-specific
    SPARQL_QUERY = "application/sparql-query"
    SPARQL_UPDATE = "application/sparql-update"
    FORM_URLENCODED = "application/x-www-form-urlencoded"
