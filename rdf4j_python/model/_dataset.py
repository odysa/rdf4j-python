from rdflib import Dataset as _Dataset

from rdf4j_python.model._term import IRI


class DataSet(_Dataset):
    def as_list(self):
        return [
            (s, p, o, ctx if ctx != IRI("urn:x-rdflib:default") else None)
            for s, p, o, ctx in self.quads((None, None, None, None))
        ]
