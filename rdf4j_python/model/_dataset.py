from rdflib import Dataset as _Dataset

from rdf4j_python.model.term import IRI


class RDF4JDataSet(_Dataset):
    """
    An RDFLib Dataset subclass with RDF4J-specific utility methods.
    """

    def as_list(self) -> list[tuple]:
        """
        Converts all quads in the dataset to a list of 4-tuples.

        Replaces the RDF4J default context IRI ("urn:x-rdflib:default") with None.

        Returns:
            list[tuple]: A list of (subject, predicate, object, context) quads.
        """
        return [
            (s, p, o, ctx if ctx != IRI("urn:x-rdflib:default") else None)
            for s, p, o, ctx in self.quads((None, None, None, None))
        ]

    @staticmethod
    def from_raw_text(text: str) -> "RDF4JDataSet":
        """
        Parses a string of N-Quads RDF data into an RDF4JDataSet.

        Args:
            text (str): The RDF data in N-Quads format.

        Returns:
            RDF4JDataSet: A populated dataset.
        """
        ds = RDF4JDataSet()
        ds.parse(data=text, format="nquads")
        return ds
