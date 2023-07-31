import re
from collections import namedtuple
from itertools import takewhile
import pandas as pd
import gspread
from rdflib import Graph, RDF, URIRef, store
import os
from model.neo4j_connector import NEO4JConnector
import rdflib_neo4j
from neo4j import GraphDatabase, Session
import json
import numpy as np


def create_local_excel(df_chains, df_groups, file_path):
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
    # main
    df_main = pd.DataFrame(columns=["id", "name", "author", "namespace", "description", "imports"])
    df_main.loc[len(df_main.index)] = ["wbrcm", "Generated neurons", "Natallia Kokash",
                                       "neurons", "Generated neuron chains from sparc-nlp.ttl",
                                       "https://raw.githubusercontent.com/open-physiology/apinatomy-models/master/models/wbrcm/source/wbrcm.json"]
    df_main.to_excel(writer, sheet_name='main', index=False)
    # lyphs, materials, chains
    # df_lyphs.to_excel(writer, sheet_name='lyphs', index=False)
    # df_materials.to_excel(writer, sheet_name='materials', index=False)
    df_chains.to_excel(writer, sheet_name='chains', index=False)
    df_groups.to_excel(writer, sheet_name='groups', index=False)
    # local conventions
    local_conventions = {
        "prefix": ["UBERON", "CHEBI", "FMA", "GO", "ILX", "NLX", "SAO", "PMID", "EMAPA", "CL", "NCBITaxon", "wbkg",
                   "too"],
        "namespace": [
            "http://purl.obolibrary.org/obo/UBERON_",
            "http://purl.obolibrary.org/obo/CHEBI_",
            "http://purl.org/sig/ont/fma/fma",
            "http://purl.obolibrary.org/obo/GO_",
            "http://uri.interlex.org/base/ilx_",
            "http://uri.neuinfo.org/nif/nifstd/nlx_",
            "http://uri.neuinfo.org/nif/nifstd/sao",
            "http://www.ncbi.nlm.nih.gov/pubmed/",
            "http://purl.obolibrary.org/obo/EMAPA_",
            "http://purl.obolibrary.org/obo/CL_",
            "http://purl.obolibrary.org/obo/NCBITaxon_",
            "https://apinatomy.org/uris/models/wbrcm/ids/",
            "https://apinatomy.org/uris/models/too-map/ids/"
        ]
    }
    df_local_conventions = pd.DataFrame(local_conventions)
    df_local_conventions.to_excel(writer, sheet_name='localConventions', index=False)
    writer.save()


# Create a dictionary of ontology terms vs lyphs from WBKG
wbkg = json.load(open('./data/wbrcm-model.json', 'r'))
lyphsByOntologyTerms = {}
lyphsByID = {}
for lyph in wbkg['lyphs']:
    lyphsByID[lyph["id"]] = lyph
    if 'ontologyTerms' in lyph:
        for ontologyTerm in lyph['ontologyTerms']:
            ontID = ontologyTerm
            if ontID in lyphsByOntologyTerms:
                print("Double", ontID + ' - ' + lyphsByOntologyTerms[ontID]['id'] + ', ' + lyph['id'])
            else:
                lyphsByOntologyTerms[ontID] = lyph

# Augment the dictionary with static mappings
lyph_map = pd.read_excel('./data/npo-nlp-apinat-location-summary.xlsx', sheet_name='npo-nlp-apinat-location-summary', dtype=str)
lyph_map = lyph_map.replace(np.nan, '')
newLyphNames = {}
rows = lyph_map.values.tolist()
for row in rows:
    ontID = row[1]
    lyphID = row[4]
    newLyphNames[ontID] = row[0]
    if lyphID:
        if ontID not in lyphsByOntologyTerms:
            if lyphID in lyphsByID:
                lyphsByOntologyTerms[ontID] = lyphsByID[lyphID]
            else:
                print("Unknown lyph: ", lyphID)
        else:
           if lyphsByOntologyTerms[ontID]["id"] != lyphID:
               print("Conflicting lyphs: ", ontID, lyphsByOntologyTerms[ontID]["id"], lyphID)

# Import .ttl to Neo4j
login = json.load(open('./data/nlp_db.json', 'r'))
db_uri, db_pwd = (login.values())

# Populate Neo4J DB (AuraDB instance)
# g = Graph(store='neo4j-cypher')
# config = {'uri': db_uri, 'database': 'neo4j', 'auth': {'user': "neo4j", 'pwd': db_pwd}}
# g.open(config, create=False)
# g.store.startBatchedWrite()
# g.parse("./data/sparc-nlp.ttl")
# g.store.endBatchedWrite()
# g.close()

# Create ApiNATOMY tables
LYPH_COLUMNS = ['id', 'ontologyTerms', 'name', 'varianceSpecs', 'isTemplate', 'topology', 'layers',
                'supertype', 'internalLyphs', 'internalLyphsInLayers', 'hostedBy']
MATERIAL_COLUMNS = ['id', 'ontologyTerms', 'name', 'materials']

CHAIN_COLUMNS = ['id', 'name', 'lyphTemplate', 'root', 'leaf', 'housingLyphs', 'levelTargets']
GROUP_COLUMNS = ['id', 'name', 'lyphs']

# df_lyphs = pd.DataFrame([], columns=LYPH_COLUMNS)
# df_materials = pd.DataFrame([], columns=MATERIAL_COLUMNS)
df_chains = pd.DataFrame([], columns=CHAIN_COLUMNS)
df_groups = pd.DataFrame([], columns=GROUP_COLUMNS)

# Access Neo4j
driver = GraphDatabase.driver(db_uri, auth=('neo4j', db_pwd))

# Get a list of neurons
cql_neurons = 'match (n: Class)-[r:neuronPartialOrder]->(m) return n'
cql_partial_order = '''
    match (src {uri:$uri})-[r: neuronPartialOrder|first|rest*..]->(dst) return r, dst
'''

unknown = []
with driver.session() as session:
    res = session.run(cql_neurons)
    neurons = [record for record in res.data()]
    for neuron in neurons:
        uri = neuron['n']['uri']
        # To create only one neuron
        # if uri != "http://uri.interlex.org/tgbugs/uris/readable/sparc-nlp/mmset1/2":
        #     continue
        print("Neuron:", uri)
        res = session.run(cql_partial_order, uri = uri)
        entries = [record for record in res.data()]

        # Simpler graph representation
        forward = {}
        # backward = {}
        start = None
        for entry in entries:
            if entry["dst"]["uri"].startswith("http://"):
                prev = None
                start = entry['r'][0][2]["uri"]
                for s in entry['r']:
                    curr = s[2]["uri"]
                    if curr not in forward:
                        forward[curr] = []
                    # if curr not in backward:
                    #     backward[curr] = []
                    if prev:
                        if curr not in forward[prev]:
                            forward[prev].append(curr)
                        # if prev not in backward[curr]:
                        #     backward[curr].append(prev)
                    prev = curr

        # Extract chains
        stack = []

        def populate(prev):
            for curr in forward[prev]:
                if curr.startswith("http://"):
                    stack.append(curr)
                populate(curr)
        populate(start)

        chains = []
        chain = []
        chain_prefix = []
        while len(stack) > 0:
            x = stack.pop()
            if x == "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil":
                if len(chain_prefix) > 0:
                    chain_prefix.pop()
                if len(chain_prefix) == 0:
                    if len(chain) > 0:
                        chains.append(chain)
                    chain = []
            else:
                chain_prefix.append(x)
                ontID = os.path.basename(x).upper().replace('_', ':')
                if ontID in lyphsByOntologyTerms:
                    ontID = "wbkg:"+lyphsByOntologyTerms[ontID]["id"]
                else:
                   if ontID not in unknown:
                       unknown.append(ontID)
                   ontID = "wbkg:lyph_" + ontID.replace(":", "_")
                chain.append(ontID)

        # Generate ApiNATOMY chains and/or groups for each extracted housingLyph set
        baseID = uri.replace("http://uri.interlex.org/tgbugs/uris/readable/sparc-nlp/", "")
        baseName = neuron['n']['label'][:50]

        if len(chains) == 1:
            lyph_str = ",".join(chains[0])
            df_chains.loc[len(df_chains.index)] = [baseID, baseName, "wbkg:lt-cell-cyst", "", "", lyph_str, ""]
            # df_groups.loc[len(df_groups.index)] = ["g_"+ baseID, "Group " + baseID, lyph_str]
        else:
            # lyph_set = set()
            # for housingLyphs in chains:
            #     for lyph in housingLyphs:
            #         lyph_set.add(lyph)
            for i, housingLyphs in enumerate(chains):
                ext = "_" + str(i+1)
                df_chains.loc[len(df_chains.index)] = [baseID + ext, baseName + ext, "wbkg:lt-cell-cyst", "", "", ",".join(housingLyphs), ""]
                # df_groups.loc[len(df_groups.index)] = ["g_"+ baseID, "Group " + baseID, ",".join(list(lyph_set))]

    # Unknown: ontologyTerms for non-existing WBKG lyphs (helped to improve WBKG)
    # print("Unknown: ", unknown)
    # for id in unknown:
    #     if id in newLyphNames:
    #         print(id, newLyphNames[id])
    #     else:
    #         print(id, 'undefined')

driver.close()

create_local_excel(df_chains, df_groups, './data/neurons.xlsx')
