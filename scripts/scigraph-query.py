import requests
import json
import urllib

url ="https://scicrunch.org/api/1/sparc-scigraph/cypher/execute.json"

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

query_arterial_supply = """
 MATCH path =
 (s:Class{iri: 'FMA:50039'})-[:fma:continuous_distally_with*0..20]->()<-[:fma:arterial_supply_of*0..1]->()
 RETURN path
"""

query_part_of = """
 MATCH path =
 (s:Class{iri: 'FMA:50039'})-[:fma:continuous_distally_with*0..20]->()<-[:fma:part_of!*0..5]-()
 RETURN path
"""
# 50039, 50040, 50010931
# r = requests.get(url,
#                  params={"cypherQuery": query, "limit": 100, "api_key": "Klm0mWxTt1djFmMlp2EUtwrzjA84ltIP"},
#                  headers={"Accept": "application/json;charset=UTF-8"}
#         )

url_uberon="https://scicrunch.org/api/1/sckan-scigraph/annotations/entities?includeCat=subcellular entity&includeCat=cell&includeCat=anatomical entity&content=" \
           "the brain is made up of cells including neurons that have dendrites"

r = requests.get(url_uberon,
                 params={"api_key": "Klm0mWxTt1djFmMlp2EUtwrzjA84ltIP"},
                 headers={"Accept": "application/json;charset=UTF-8"}
        )
print(r.text)
js = r.json()
print("JSON", js, type(js))

# /scigraph/vocabulary/term/skin%20of%20finger?limit=10

# /scigraph/vocabulary/term/skin%20of%20finger?limit=10&prefix=UBERON

# https://github.com/SciGraph/SciGraph/wiki/Cypher-language-extension
# https://fdilab.gitbook.io/api-handbook/sparc-kg-scigraph/endpoints
# https://scicrunch.org/api/1/sckan-scigraph/docs/?url=https://scicrunch.org/api/1/sckan-scigraph/swagger.json
# /cypher/execute
#  MATCH path =
#  (s:Class{iri: $anat_id})
#  -[:fma:continuous_distally_with*0..20]->()
#  <-[:fma:part_of!*0..5]-()
#  // -[:fma:arterial_supply_of*0..1]->()  // very sparse
#  RETURN path
# http://selene:9000/scigraph/cypher/execute.json?limit=999999&cypherQuery=MATCH%20path%20%3D%0A%28s%3AClass%7Biri%3A%20%24anat_id%7D%29%0A-%5B%3Afma%3Acontinuous_distally_with%2A0..20%5D-%3E%28%29%0A%3C-%5B%3Afma%3Apart_of%21%2A0..5%5D-%28%29%0A%2F%2F%20-%5B%3Afma%3Aarterial_supply_of%2A0..1%5D-%3E%28%29%20%20%2F%2F%20very%20sparse%0ARETURN%20path%3B&anat_id=FMA:3734
# curl -sH 'Accept: application/json; charset=UTF-8'
# https://scicrunch.org/api/1/sparc-scigraph/cypher/execute.json?limit=999999&cypherQuery=MATCH%20path%20%3D%0A%28s%3AClass%7Biri%3A%20%24anat_id%7D%29%0A-%5B%3Afma%3Acontinuous_distally_with%2A0..20%5D-%3E%28%29%0A%3C-%5B%3Afma%3Apart_of%21%2A0..5%5D-%28%29%0A%2F%2F%20-%5B%3Afma%3Aarterial_supply_of%2A0..1%5D-%3E%28%29%20%20%2F%2F%20very%20sparse%0ARETURN%20path%3B&anat_id=FMA:3734

# https://fdilab.gitbook.io/api-handbook/sparc-kg-scigraph/endpoints

# https://scicrunch.org/api/1/sckan-scigraph/annotations?content=T5%20spinal%20cord%20and%20reaches%20the%20spleen%20via%20the%20hyogastric%20nerve

# Documentation for querying SciGraph
# https://github.com/tgbugs/pyontutils/blob/3fe47bb0609920f76359470a510be63d79843a0a/nifstd/scigraph/graphload-base-template.yaml#L20-L36
# https://scicrunch.org/api/1/sckan-scigraph/annotations?includeCat=cell&includeCat=anatomical%20entity&content=the%20brain%20is%20made%20up%20of%20cells%20including%20neurons

# https://scicrunch.org/api/1/sckan-scigraph/annotations?includeCat=subcellular entity&includeCat=cell&includeCat=anatomical entity&content=the brain is made up of cells including neurons that have dendrites