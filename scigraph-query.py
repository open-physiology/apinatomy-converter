import requests
import json

url ="https://scicrunch.org/api/1/sparc-scigraph/cypher/execute"

query_neuron = """
    MATCH (neuron)-[e]->
    (blanka)-[:ilxtr:hasAxonLocatedIn]->(blankb)-[edge]->(region_or_blank)
    RETURN edge
"""

query_all_pop="""
    OPTIONAL MATCH (start:Ontology)
    <-[:isDefinedBy]-(graph:NamedIndividual)
    -[:type]->({iri: "https://apinatomy.org/uris/elements/Graph"})
    , (start)
    <-[:isDefinedBy]-(external:Class)
    -[:subClassOf*]->(:Class {iri: "http://uri.interlex.org/tgbugs/uris/readable/NeuronEBM"})
    return external
"""

query_apinatomy_graph = """
    MATCH ({iri: "https://apinatomy.org/uris/elements/Graph"})<-[:type]-(g)<-[e:apinatomy:hasGraph]-(o:Ontology) RETURN g, e, o
"""

r = requests.get(url, params={"cypherQuery": query_apinatomy_graph, "limit": 10, "api_key": "Klm0mWxTt1djFmMlp2EUtwrzjA84ltIP"})
print(r.text)