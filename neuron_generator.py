import pandas as pd
import os
import gspread
from rdflib import Graph, RDF, URIRef, store
import rdflib_neo4j
from neo4j import GraphDatabase
import json
import numpy as np


# Create a spreadsheet ApiNATOMY model
def create_local_excel(df_chains, df_groups, df_lyphs, df_links, file_path):
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
    # main
    df_main = pd.DataFrame(columns=["id", "name", "author", "namespace", "description", "imports"])
    df_main.loc[len(df_main.index)] = ["sparc-nlp", "Generated neurons", "Natallia Kokash",
                                       "nlp", "Generated neuron chains from sparc-nlp.ttl",
                                       "https://raw.githubusercontent.com/open-physiology/apinatomy-models/master/models/wbrcm/source/wbrcm.json"]
    df_main.to_excel(writer, sheet_name='main', index=False)
    df_lyphs.to_excel(writer, sheet_name='lyphs', index=False)
    df_links.to_excel(writer, sheet_name='links', index=False)
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
def create_ontology_term_dict():
    wbkg = json.load(open('./data/wbrcm-model.json', 'r'))
    lyphsByID = {}
    for lyph in wbkg['lyphs']:
        lyphsByID[lyph["id"]] = lyph
        if 'ontologyTerms' in lyph:
            for ontologyTerm in lyph['ontologyTerms']:
                ontID = ontologyTerm
                if ontID not in lyphsByURIs:
                    lyphsByURIs[ontID] = lyph
                # else:
                #     print("Double", ontID + ' - ' + lyphsByURIs[ontID]['id'] + ', ' + lyph['id'])

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
            if ontID not in lyphsByURIs:
                if lyphID in lyphsByID:
                    lyphsByURIs[ontID] = lyphsByID[lyphID]
                else:
                    print("Unknown lyph: ", lyphID)
            else:
               if lyphsByURIs[ontID]["id"] != lyphID:
                   print("Conflicting lyphs: ", ontID, lyphsByURIs[ontID]["id"], lyphID)
    return lyphsByURIs


# Assign conveying lyph supertype
def assign_neuron_segment_lyph_tempates():
    df_templates = pd.read_excel('./data/npo-by-predicates.xlsx', sheet_name='npo-by-predicates', dtype=str)
    df_templates = df_templates.replace(np.nan, '')

    TEMPLATE_COLUMNS = ['id', 'ilxtr:hasAxonLocatedIn', "ilxtr:hasAxonPresynapticElementIn",
                        "ilxtr:hasAxonSensorySubcellularElementIn", "ilxtr:hasSomaLocatedIn"]
    df_templates = df_templates[TEMPLATE_COLUMNS]

    # [:1] is to skip first row where Bernard added column annotations
    template_rows = df_templates.values.tolist()[1:]
    template_map = {}
    for row in template_rows:
        template_map[row[0]] = {
            "lt-axon-tube": row[1].split(',') if len(row[1]) > 0 else [],
            "lt-axon-bag": row[2].split(',') if len(row[2]) > 0 else [],
            "lt-dend-bag": row[3].split(',') if len(row[3]) > 0 else [],
            "lt-soma-of-neuron": row[4].split(',') if len(row[4]) > 0 else []
        }

    for value in template_map:
        for prop in template_map[value]:
            template_map[value][prop] = [x for x in template_map[value][prop]]

    # for value in template_map:
    #     print(value,template_map[value])

    # Assign neuron segment topology:
    # lt-axon-tube, lt-axon-bag, (lt-axon-bag2)
    # lt-dend-tube, lt-dend-bag, (lt-dend-bag2)
    for value in template_map:
        lyph_to_template[value] = {}
        for prop in template_map[value]:
            for lyph in template_map[value][prop]:
                lyph_to_template[value][lyph] = "wbkg:" + prop
    # for value in lyph_to_template:
    #     print(value)
    #     for lyph in lyph_to_template[value]:
    #         print(lyph, lyph_to_template[value][lyph])


# Assign neuron segment conveying lyphs to housing lyphs
def get_supertypes(uri, housingLyphs):
    supertypes = {}
    default_segment = "wbkg:lt-segment-of-neuron"
    for lyph in housingLyphs:
        if uri in lyph_to_template and lyph in lyph_to_template[uri]:
            supertypes[lyph] = lyph_to_template[uri][lyph]
        else:
            supertypes[lyph] = default_segment
    return supertypes


lyphsByURIs = {}
create_ontology_term_dict()

# print(len(lyphsByURIs))
# for key in lyphsByURIs:
#     print(key, ' -> ', lyphsByURIs[key]["id"])


lyph_to_template = {}
assign_neuron_segment_lyph_tempates()


# Access .ttl imported to Neo4j
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


# Extract URI chains of housing lyphs from Neo4j/.ttl graph
def extract_housing_lyphs():
    # Simpler graph representation
    forward = {}
    start = None
    for entry in entries:
        if entry["dst"]["uri"].startswith("http://"):
            prev = None
            start = entry['r'][0][2]["uri"]
            for s in entry['r']:
                curr = s[2]["uri"]
                if curr not in forward:
                    forward[curr] = []
                if prev:
                    if curr not in forward[prev]:
                        forward[prev].append(curr)
                prev = curr

    # Extract chains
    stack = []

    def populate(prev):
        for curr in forward[prev]:
            if curr.startswith("http://"):
                stack.append(curr)
            populate(curr)

    populate(start)

    unknown = []
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
            if ontID not in lyphsByURIs:
                if ontID not in unknown:
                    unknown.append(ontID)
                # Generate a fake lyph not to miss a chain level
                lyphsByURIs[ontID] = {
                    "id": "wbkg:lyph_" + ontID.replace(":", "_")
                }
            chain.append(ontID)
    # Unknown: ontologyTerms for non-existing WBKG lyphs (helped to improve WBKG)
    # print("Unknown: ", unknown)
    # for id in unknown:
    #     if id in newLyphNames:
    #         print(id, newLyphNames[id])
    #     else:
    #         print(id, 'undefined')
    return chains


# Create ApiNATOMY tables
LYPH_COLUMNS = ['id', 'name', 'supertype']
LINK_COLUMNS = ['id', 'name', 'conveyingLyph', 'source', "target"]
CHAIN_COLUMNS = ['id', 'name', 'housingLyphs', 'lyphs', 'levels']
GROUP_COLUMNS = ['id', 'name', 'lyphs', "links", "nodes"]

df_lyphs = pd.DataFrame([], columns=LYPH_COLUMNS)
df_links = pd.DataFrame([], columns=LINK_COLUMNS)
df_chains = pd.DataFrame([], columns=CHAIN_COLUMNS)
df_groups = pd.DataFrame([], columns=GROUP_COLUMNS)

# Access Neo4j
driver = GraphDatabase.driver(db_uri, auth=('neo4j', db_pwd))

# Get a list of neurons
cql_neurons = 'match (n: Class)-[r:neuronPartialOrder]->(m) return distinct n'
cql_partial_order = '''
    match (src {uri:$uri})-[r: neuronPartialOrder|first|rest*..]->(dst) return r, dst
'''

# Read nlp data and convert to apiNATOMY chains
with driver.session() as session:
    res = session.run(cql_neurons)
    neurons = [record for record in res.data()]
    print(len(neurons))
    file_ext = ""
    for neuron in neurons:
        uri = neuron['n']['uri']
        print("Found neuron:", uri)
        baseID = uri.replace("http://uri.interlex.org/tgbugs/uris/readable/sparc-nlp/", "")
        baseName = neuron['n']['label'][:50] # Too long names get into group names and do not fit the Setting Panel
        # To create only one neuron
        if uri != "http://uri.interlex.org/tgbugs/uris/readable/sparc-nlp/prostate/13":
            continue
        else:
            file_ext = "_" + baseID.replace("/", "_")
        print("Processing neuron: ", uri)

        # extract chains of housing lyphs
        res = session.run(cql_partial_order, uri=uri)
        entries = [record for record in res.data()]
        chains = []
        extract_housing_lyphs()

        housing_ont_set = set()
        for housingLyphs in chains:
            for lyph in housingLyphs:
                housing_ont_set.add(lyph)

        host_to_supertypes = get_supertypes(uri, housing_ont_set)
        # print("Supertypes: ", len(host_to_supertypes))

        # Generate ApiNATOMY chains and/or groups for each extracted housingLyph set

        if len(chains) > 1:
            # Complex assembly, we will follow a set of extracted chains of housing lyphs and generate
            # the minimal possible number of neuron segments to ensure correct ApiNATOMY chain connectivity, i.e.,
            # each previous level should end in the same node as the current segment starts

            # Initially create one neuron segment per unique housing lyph URI
            host_to_links = {}
            for i, host in enumerate(host_to_supertypes):
                host_to_links[host] = []
                host_to_links[host].append(
                    {
                        "id": baseID + "_lnk_" + str(i),
                        "name": "Neural segment for " + host,
                        # "conveyingLyph": gen_lyph_id
                        "conveyingLyph": host_to_supertypes[host]
                    }
                )
            # print("Unique segments: ", len(host_to_links))

            idx = len(host_to_links)
            idx_n = 1
            for i, housing_lyphs in enumerate(chains):
                levels = []
                node_pairs = []

                for j, host in enumerate(housing_lyphs):
                    # neuron link before current
                    prev = levels[j - 1] if j > 0 else None
                    # next housing lyph
                    following = housing_lyphs[j + 1] if j < len(housing_lyphs) - 1 else None
                    # available links for the next housing lyph
                    next_all = host_to_links[following] if following else []
                    # available links for the next housing lyph that start where the previous segment ends
                    selected = [x for x in host_to_links[host] if "source" not in x or x["source"] == prev["target"]]
                    if len(selected) > 1:
                        # Many options, we take the last one
                        selected = selected[-1:]
                    if len(selected) == 0:
                        # No suitable links, a new link needed
                        selected = [{
                            "id": baseID + "_lnk_" + str(idx),
                            "name": "Neural segment for " + host,
                            # "conveyingLyph": gen_lyph_id
                            "conveyingLyph": host_to_supertypes[host],
                            "source": prev["target"]
                        }]
                        idx += 1
                    for curr in selected:
                        lnk = curr
                        if "source" in curr and "target" in curr:
                            # We reached a housing lyph in a chain for which neural segment ends are defined,
                            # need to check if it is suitable or reroute (create another link)
                            match = [x for x in next_all if "source" in x and x["source"] == curr["target"]]
                            if len(match) == 0:
                                free = [x for x in next_all if "source" not in x]
                                if len(free) > 1:
                                    # There should not be more than one link with undefined source
                                    print("Something weird...", j)
                                if len(free) > 0 and ("target" not in free[0] or free[0]["target"] != curr["target"]):
                                    # If there is a free link (with undefined source and no conflict), assign its source
                                    free[0]["source"] = curr["target"]
                                else:
                                    # no suitable continuation of the neuron chain
                                    if len(next_all) > 1:
                                        # Can several options exist for the next segment???
                                        # We will connect the current segment to the first below
                                        print("Several links exist for the segment...", j, host)
                                    if len(match) == 0 and len(next_all) > 0:
                                        source = curr["source"]
                                        # Check that the source matches previous level
                                        if prev and prev["target"] != source:
                                            source = prev["target"]
                                        if source != next_all[0]["source"]:
                                            target = next_all[0]["source"]
                                        else:
                                            target = baseID + "_n" + str(idx_n)
                                            idx_n += 1
                                        lnk = {
                                            "id": baseID + "_lnk_" + str(idx),
                                            "name": "Neural segment for " + host,
                                            # "conveyingLyph": gen_lyph_id
                                            "conveyingLyph": host_to_supertypes[host],
                                            "source": source,
                                            "target": target
                                        }
                                        idx += 1
                                        host_to_links[host].append(lnk)
                        else:
                            # Neural segment links we allocated for each housing lyph are not connected yet,
                            # we join them following the chains of housing lyphs and assigning source and target nodes
                            # from previous and/or next segments
                            if "source" not in curr:
                                if prev:
                                    curr["source"] = prev["target"]
                                else:
                                    curr["source"] = baseID + "_n" + str(idx_n)
                                    idx_n += 1
                            if "target" not in curr:
                                next_lnk = host_to_links[following][0] if following else None
                                # all next hops should have the same entry point?
                                if next_lnk and "source" in next_lnk:
                                    curr["target"] = next_lnk["source"]
                                else:
                                    curr["target"] = baseID + "_n" + str(idx_n)
                                    idx_n += 1
                        levels.append(lnk)
                        node_pairs.append(host + ":{" + lnk["source"] + ", " + lnk["target"] + "}")
                    # print(j, [x for x in node_pairs])

                ext = "_" + str(i+1)
                levels_str = ','.join([x["id"] for x in levels])

                housings = ["wbkg:" + lyphsByURIs[x]["id"] for x in housing_lyphs if x in lyphsByURIs]
                housing_lyphs_str = ','.join(housings)

                # for j, np in enumerate(node_pairs):
                #     print(j, np)
                # print()

                df_chains.loc[len(df_chains.index)] = [baseID+ext, baseName+ext, housing_lyphs_str, "", levels_str]

            # Collect link and node sets for a joint assembly group
            lnk_set = set()
            node_set = set()
            for host in host_to_links:
                for lnk in host_to_links[host]:
                    df_links.loc[len(df_links.index)] = lnk.values()
                    lnk_set.add(lnk["id"])
                    node_set.add(lnk["source"])
                    node_set.add(lnk["target"])
            lnk_set_str = ",".join(["nlp:" + x for x in lnk_set])
            node_set_str = ','.join(set(["nlp:" + x for x in node_set]))

            # Create neuron group
            df_groups.loc[len(df_groups.index)] = ["g_neuron_"+baseID, "Group "+baseID, "", lnk_set_str, node_set_str]
        else:
            # Single chain, ApiNATOMY chain is defined by lyphs and housingLyphs
            lyphs = []
            for i, host in enumerate(chains[0]):
                # To generate lyph instances instead of abstract lyphs
                gen_lyph_id = baseID + "_lyph_" + str(i)
                lyphs.append(gen_lyph_id)
                df_lyphs.loc[len(df_lyphs.index)] = [gen_lyph_id, "Neural segment for " + host, host_to_supertypes[host]]

            lyphs_str = ','.join(lyphs)
            housing_lyphs_str = ','.join(["wbkg:"+lyphsByURIs[x]["id"] for x in chains[0] if x in lyphsByURIs])
            df_chains.loc[len(df_chains.index)] = [baseID, baseName, housing_lyphs_str, lyphs_str, ""]

        # Housing group - useless until open-physiology-viewer handles group extensions from other namespace resources
        # housing_lyphs_set_str = ",".join(["wbkg:"+lyphsByURIs[x]["id"] for x in housing_ont_set if x in lyphsByURIs])
        # df_groups.loc[len(df_groups.index)] = ["g_" + baseID, "Housing group " + baseID, housing_lyphs_set_str, "", ""]

driver.close()

print('./data/neurons' + file_ext + '.xlsx')
create_local_excel(df_chains, df_groups, df_lyphs, df_links, './data/neurons' + file_ext + '.xlsx')