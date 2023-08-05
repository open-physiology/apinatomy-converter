from rdflib import Graph, Namespace, BNode

# Load the Turtle data from the file
file_path = "Data/sparc-nlp.ttl"
graph = Graph()
graph.parse(file_path, format="turtle")

# Define the ilxtr namespace
ilxtr = Namespace("http://uri.interlex.org/tgbugs/uris/readable/")

# Query elements with ilxtr:neuronPartialOrder property
query = """
SELECT ?element ?value
WHERE {
  ?element ilxtr:neuronPartialOrder ?value .
}
"""

results = graph.query(query)

def retrieve_bnode_properties(bnode):
    bnode_representation = {}
    triples_with_bnode_as_subject = list(graph.triples((bnode, None, None)))
    for s, p, o in triples_with_bnode_as_subject:
        predicate = p.split("#")[-1]  # Extract the predicate name without the namespace
        if isinstance(o, BNode):
            object_value = retrieve_bnode_properties(o)  # Recursive call for nested BNode
        else:
            object_value = o.toPython() if isinstance(o, (int, float)) else str(o)
        bnode_representation[predicate] = object_value
    return bnode_representation

    
# Print the neuronPartialOrder content for each element
for row in results:
    element = row['element']
    bnode = row['value']
    # Retrieve properties and values of the BNode and its nested BNodes recursively
    bnode_representation = retrieve_bnode_properties(bnode)

    # Print the representation of the BNode
    print("BNode representation:")
    print(bnode_representation)
    
    