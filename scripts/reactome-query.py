from neo4j import GraphDatabase
import csv
import json

login = json.load(open('credentials/reactom_neo4j_db.json', 'r'))
neo4j_uri, neo4j_pwd = (login.values())

driver = GraphDatabase.driver(neo4j_uri, auth=("neo4j", neo4j_pwd))


def expand_complex(session, pe_stId):
    cql_get_components = """
                        match  path = (n:PhysicalEntity)-[r2:(hasComponent|hasMember)*]->(m) where n.stIdVersion=$pe_stId
                            UNWIND relationShips(path) AS r
                            WITH collect(DISTINCT endNode(r))   AS endNodes, 
                                 collect(DISTINCT startNode(r)) AS startNodes
                            UNWIND endNodes AS leaf
                            WITH leaf WHERE NOT leaf IN startNodes
                            MATCH (leaf)-[:compartment]-(c)
                            RETURN leaf, c"""
    res = session.run(cql_get_components, pe_stId=pe_stId)
    components = [record for record in res.data()]
    for c in components:
        print('\t\t', c['leaf']["stIdVersion"], c['leaf']["schemaClass"], c['leaf']["displayName"], c['c']['url'])


def get_colabamin_pathway_reactions():
    with driver.session() as session:
        cql_get_b12_stId = 'match (p: Pathway) where p.speciesName = "Homo sapiens" and p.displayName starts with "Cobalamin"' \
                      ' return p.stIdVersion as id order by p.releaseDate desc limit(1)'
        res = session.run(cql_get_b12_stId)
        b12 = [record for record in res.data()]
        b12_stId = b12[0]["id"]

        cql_get_reactions = "match (p: Pathway)-[]->(n: ReactionLikeEvent) where p.stIdVersion=$stId return n"
        res = session.run(cql_get_reactions, stId=b12_stId)
        reactions = []
        for record in [r['n'] for r in res.data()]:
            if record["schemaClass"] == "Reaction":
                reactions.append(record)

            def get_reactions(st_id):
                cql_get_bbe_reactions = 'match (b: ReactionLikeEvent)-[]->(n: ReactionLikeEvent {speciesName: "Homo sapiens"}) ' \
                                        'where b.stIdVersion = $bbe_stId return n'
                res2 = session.run(cql_get_bbe_reactions, bbe_stId=st_id)
                for record2 in [r['n'] for r in res2.data()]:
                    if record2["schemaClass"] == "Reaction":
                        reactions.append(record2)
                    else:
                        get_reactions(record2["stIdVersion"])

            get_reactions(record["stIdVersion"])

        print(len(reactions))

        for r in reactions:
            r_stId = r['stIdVersion']
            print("REACTION: ", r['stIdVersion'], r['displayName'])
            cql_get_input_output='match (n:Reaction)-[r2:input|output]->(m)-[:compartment]->(c) where n.stIdVersion=$r_stId  return m, c'
            res = session.run(cql_get_input_output, r_stId=r_stId)
            ios = [record for record in res.data()]
            if len(ios) > 0:
                print("input/output: ")
            for pe in ios:
                if pe['m']["schemaClass"] == 'Complex':
                    pe_stId = pe['m']["stIdVersion"]
                    # print('\t', io['m']["stIdVersion"], io['m']["displayName"])
                    expand_complex(session, pe_stId)
                else:
                    print('\t\t', pe['m']["stIdVersion"], pe['m']["schemaClass"], pe['m']["displayName"], pe['c']['url'])

            cql_get_input_output = """
                match (n:Reaction)-[:catalystActivity]->(m)-[:physicalEntity]->(k:PhysicalEntity)-[:compartment]->(c)  
                where n.stIdVersion=$r_stId return m,k,c"""
            res = session.run(cql_get_input_output, r_stId=r_stId)
            cat = [record for record in res.data()]
            if len(cat) > 0:
                print("catalyst: ")
            for pe in cat:
                print('\t', pe['m']['displayName'])
                if pe['k']['schemaClass'] == 'Complex':
                    pe_stId = pe['k']["stIdVersion"]
                    # print('\t', io['k']["stIdVersion"], io['k']["displayName"])
                    expand_complex(session, pe_stId)
                else:
                    print('\t\t', pe['k']['stIdVersion'], pe['k']['schemaClass'], pe['k']['displayName'], pe['c']['url'])
    driver.close()


def expand_complex_uniprot(session, pe_stId):
    cql_get_components = """
                        match path = (n:PhysicalEntity)-[r2:(hasComponent|hasMember)*]->(m) where n.stIdVersion=$pe_stId
                            UNWIND relationShips(path) AS r
                            WITH collect(DISTINCT endNode(r))   AS endNodes, 
                                 collect(DISTINCT startNode(r)) AS startNodes
                            UNWIND endNodes AS leaf
                            WITH leaf WHERE NOT leaf IN startNodes
                            MATCH (leaf)-[:referenceEntity]-(db {databaseName: "UniProt"})
                            RETURN leaf, db"""
    res = session.run(cql_get_components, pe_stId=pe_stId)
    components = [record for record in res.data()]
    rows = []
    for c in components:
        print('\t\t', c['leaf']["stIdVersion"], c['leaf']["displayName"], c['db']['identifier'], c['db']['displayName'])
        row = [c['leaf']["stIdVersion"], c['leaf']["displayName"], c['db']['identifier'], c['db']['displayName']]
        rows.append(row)
    return rows


def get_sodium_reactions():
    with driver.session() as session:
        cql_get_reactions = 'match (db1: DatabaseObject {databaseName: "ChEBI", identifier:"29101"})-' \
                            '[:referenceEntity]-(i:SimpleEntity)-[:input]-(r:Reaction)-[:output]-(o:SimpleEntity)-' \
                            '[:referenceEntity]-(db2: DatabaseObject {databaseName: "ChEBI", identifier:"29101"}) return r'
        res = session.run(cql_get_reactions)
        reactions = []
        for record in [r['r'] for r in res.data()]:
            if record["schemaClass"] == "Reaction":
                reactions.append(record)

        rows = []
        for r in reactions:
            r_stId = r['stIdVersion']
            r_displayName = r['displayName']
            print(r_stId, r_displayName)

            cql_get_catalyst_uniprot = """
                call {match (r:Reaction)-[:catalystActivity]-(ca:CatalystActivity)-[:physicalEntity]-(pe:PhysicalEntity) 
                where r.stIdVersion=$r_stId return r,ca,pe} with r,ca,pe
                optional match (pe)-[:referenceEntity]->(db {databaseName: "UniProt"}) return r, ca, pe, db"""
            res = session.run(cql_get_catalyst_uniprot, r_stId=r_stId)
            cat = [record for record in res.data()]
            for pe in cat:
                print('\t', pe['ca']['displayName'])
                if pe['pe']['schemaClass'] == 'EntityWithAccessionedSequence':
                    print("\t\tdirect:")
                    print('\t\t', pe['pe']['stIdVersion'], pe['pe']['displayName'], pe['db']['identifier'], pe['db']['displayName'])
                    rows.append([r_stId, r_displayName, pe['pe']['stIdVersion'], pe['pe']['displayName'], pe['db']['identifier'], pe['db']['displayName']])
                else:
                    # if pe['pe']['schemaClass'] == 'Complex':
                    pe_stId = pe['pe']["stIdVersion"]
                    print(f"\t\tcomplex {pe['pe']['schemaClass']}")
                    complex = expand_complex_uniprot(session, pe_stId)
                    for c in complex:
                        c1 = [r_stId, r_displayName] + c
                        rows.append(c1)

        with open("../data/sodium.csv", 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)


def get_uniprot_list():
    glygen_ids = [
        "Q9H553-1", "Q9BYC5-1", "Q11203-1", "Q11206-1", "Q9Y274-1", "P26572-1", "O60512-1", "O60909-1", "P15291-1",
        "Q10469-1", "Q9H3H5-1", "Q96F25-1", "Q9NP73-1", "Q9BT22-1", "Q9H553-1", "O60512-1", "O60909-1", "P15291-1", "Q9BYC5-1",
        "Q11203-1", "Q11206-1", "Q9Y274-1", "P26572-1", "Q10469-1", "Q9UBM8-1", "Q9UM21-1", "Q9UQ53-1", "Q09328-1", "Q3V5L5-1",
        "Q9H3H5-1", "Q96F25-1", "Q9NP73-1", "Q9BT22-1", "Q9H553-1", "Q92685-1", "Q2TAA5-1", "Q9BV10-1", "Q9H6U8-1", "Q9H3H5-1",
        "Q96F25-1", "Q9NP73-1", "Q9BT22-1", "Q9H553-1", "O60512-1", "O60909-1", "P15291-1", "P26572-1", "Q10469-1", "Q9UBM8-1",
        "Q9UM21-1","Q9UQ53-1", "Q09328-1", "Q3V5L5-1", "Q9H3H5-1", "Q96F25-1", "Q9NP73-1", "Q9BT22-1", "Q9H553-1", "O60512-1",
        "O60909-1","P15291-1", "Q9BYC5-1", "Q11203-1", "Q11206-1", "Q9Y274-1", "P26572-1", "Q10469-1", "Q7Z7M8-1", "Q9C0J1-1",
        "Q9NY97-1", "Q9UBM8-1", "Q9UM21-1", "Q9UQ53-1", "Q09328-1", "Q3V5L5-1", "Q9H3H5-1", "Q96F25-1", "Q9NP73-1", "Q9BT22-1",
        "Q9H553-1",
        "O60512-1", "O60909-1", "P15291-1", "Q00973-1", "Q8NHY0-1", "P15907-1", "Q96JF0-1", "Q9BYC5-1", "Q11203-1",
        "Q11206-1",
        "Q9Y274-1", "P26572-1", "Q10469-1", "Q9UBM8-1", "Q9UM21-1", "Q9UQ53-1", "Q09328-1", "Q3V5L5-1", "Q9H3H5-1",
        "Q96F25-1",
        "Q9NP73-1", "Q9BT22-1", "Q9H553-1", "P15907-1", "Q96JF0-1", "P21217-1", "P22083-1", "P51993-1", "Q11128-1",
        "Q9Y231-1",
        "P26572-1", "O60512-1", "O60909-1", "P15291-1", "Q11203-1", "Q11206-1", "Q9Y274-1", "Q10469-1", "Q9UBM8-1",
        "Q9UM21-1",
        "Q9UQ53-1", "Q9H3H5-1", "Q96F25-1", "Q9NP73-1", "Q9BT22-1", "Q9H553-1", "O60512-1", "O60909-1", "P15291-1",
        "P21217-1",
        "P22083-1", "P51993-1", "Q11128-1", "Q9Y231-1", "P26572-1", "Q10469-1", "Q9UBM8-1", "Q9UM21-1", "Q9UQ53-1",
        "Q09328-1",
        "Q3V5L5-1", "Q9H3H5-1", "Q96F25-1", "Q9NP73-1", "Q9BT22-1", "Q9H553-1", "Q9BYC5-1", "P26572-1", "O60512-1",
        "O60909-1",
        "P15291-1", "Q10469-1", "P19526-1", "Q10981-1", "P16442-1", "Q9H3H5-1", "Q96F25-1", "Q9NP73-1", "Q9BT22-1",
        "Q9H553-1",
        "Q09327-1", "P15907-1", "Q96JF0-1", "Q9BYC5-1", "P26572-1", "O60512-1", "O60909-1", "P15291-1", "Q10469-1",
        "P19526-1",
        "Q10981-1", "Q9H3H5-1", "Q96F25-1", "Q9NP73-1", "Q9BT22-1"]

    db_ids = [id[:-2] for id in set(glygen_ids)]
    db_ids.sort()
    print(len(db_ids), "unique ids out of", len(glygen_ids))

    with driver.session() as session:
        rows = []
        cql_get_physical_entities = """MATCH 
            (r: Reaction)-[:input|output|catalystActivity]->()-[*1..3]->(pe:EntityWithAccessionedSequence)-
            [:referenceEntity]->(db: DatabaseObject {databaseName: "UniProt", identifier: $db_id}) RETURN r, db"""
        for db_id in db_ids:
            res = session.run(cql_get_physical_entities, db_id=db_id)
            reactions = [record for record in res.data()]
            print("Reactions associated with", db_id, len(reactions))
            for r in reactions:
                rows.append(["https://reactome.org/content/detail/" + r['r']['stIdVersion'], r['r']['displayName'],
                             r['r']['speciesName'], db_id, r['db']['url'], r['db']['displayName']])

        with open("../data/glygen.csv", 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)

# get_sodium_reactions()
# get_uniprot_list()


def reactome_to_data_distillery():
    file = open("../data/reactome/dd_reactome-links.csv", 'w', encoding='utf-8', newline='')
    writer = csv.writer(file)

    with driver.session() as session:
        pe_map = {}

        # Same as process_physical_entity but in 1 query
        def expand_data_distillery(pe_st_id):
            if pe_st_id not in pe_map:
                cql_get_components = """
                                    match  path = (n:PhysicalEntity)-[r2:(hasComponent|hasMember)*]->(m) where n.stId=$pe_stId
                                        UNWIND relationShips(path) AS r
                                        WITH collect(DISTINCT endNode(r))   AS endNodes, 
                                             collect(DISTINCT startNode(r)) AS startNodes
                                        UNWIND endNodes AS leaf
                                        WITH leaf WHERE NOT leaf IN startNodes
                                        MATCH (db)<-[:referenceEntity]-(leaf)-[:compartment]->(c) where 
                                            db.databaseName="UniProt" or db.databaseName="ChEBI"
                                        RETURN leaf, c, db
                                        """
                res = session.run(cql_get_components, pe_stId=pe_st_id)
                components = [record for record in res.data()]
                leaves = []
                for entry in components:
                    row = [
                        entry['db']['dbId'], entry['db']['url'], entry['db']['displayName'],
                        entry['c']['dbId'], entry['c']['url'], entry['c']['displayName']]
                    # print("\t\t", row)
                    leaves.append(row)
                pe_map[pe_st_id] = leaves
            return pe_map[pe_st_id]

        rows = []

        # Expand one by one
        # def expand_complex(pe_id, r):
        #     # print("Complex: ", pe_id)
        #     cql = """match (n:PhysicalEntity)-[:hasComponent]->(pe) where n.stId=$pe_stId return pe"""
        #     res = session.run(cql, pe_stId=pe_id)
        #     ios = [record for record in res.data()]
        #     for entry in ios:
        #         process_physical_entity(entry['pe']['schemaClass'], entry['pe']['stId'], r)
        #
        # def expand_defined_set(pe_id, r):
        #     # print("Defined set: ", pe_id)
        #     cql = """match (n:PhysicalEntity)-[:hasMember]->(pe) where n.stId=$pe_stId return pe"""
        #     res = session.run(cql, pe_stId=pe_id)
        #     ios = [record for record in res.data()]
        #     for entry in ios:
        #         process_physical_entity(entry['pe']['schemaClass'], entry['pe']['stId'], r)
        #
        # def process_leaf(pe_id, r):
        #     # print("Leaf: ", pe_id)
        #     # cql = """match (c)<-[:compartment]-(n:SimpleEntity|EntityWithAccessionedSequence)-[:referenceEntity]->(db)
        #     #     where db.databaseName="UniProt" or db.databaseName="ChEBI" and n.stId=$pe_stId return c, db"""
        #     cql = """
        #         call {match (n:SimpleEntity|EntityWithAccessionedSequence) where n.stId=$pe_stId return n}
        #         with n match (c)<-[:compartment]-(n)-[:referenceEntity]->(db) where
        #             db.databaseName="UniProt" or db.databaseName="ChEBI" return c, n, db
        #     """
        #     res = session.run(cql, pe_stId=pe_id)
        #     ios = [record for record in res.data()]
        #     for entry in ios:
        #        row = r + [
        #            entry['db']['dbId'], entry['db']['url'], entry['db']['displayName'],
        #            entry['c']['dbId'], entry['c']['url'], entry['c']['displayName']]
        #        print("\t\t", row)
        #        rows.append(row)
        #
        # def process_physical_entity(pe_class, pe_id, r):
        #     # print("Switch: ", pe_class, pe_id)
        #     if pe_class == "Complex":
        #         expand_complex(pe_id, r)
        #     elif pe_class == "DefinedSet":
        #         expand_defined_set(pe_id, r)
        #     else:
        #         process_leaf(pe_id, r)

        def process_physical_entities(cql):
            res = session.run(cql)
            ios = [record for record in res.data()]
            for pe in ios:
                r = [pe['r']['stId'], pe['r']['displayName']]
                # print("\tReaction: ", r)
                # process_physical_entity(pe['pe']["schemaClass"], pe['pe']["stId"], r)
                leaves = expand_data_distillery(pe['pe']["stId"])
                for leave in leaves:
                    row = r + leave
                    print("\t\t", row)
                    writer.writerow(row)
                    rows.append(row)

        # Extract PhysicalEntities for reaction's input and output

        try:
            print("CATALYST ACTIVITIES:")
            cql_catalyst_activities = """match (r:Reaction)-[:catalystActivity]->(ca:CatalystActivity)-[:physicalEntity]->
                        (pe:EntityWithAccessionedSequence|SimpleEntity|Complex|DefinedSet) return r, pe order by r.stId"""
            process_physical_entities(cql_catalyst_activities)
            print(len(rows))

            print("INPUT")
            cql_input = """match (r:Reaction)-[:input]->
                (pe:EntityWithAccessionedSequence|SimpleEntity|Complex|DefinedSet) return r, pe order by r.stId"""
            process_physical_entities(cql_input)
            print(len(rows))
        except Exception as e:
            print(e)
            print("Processed rows:", len(rows))

        # with open("data/dd_reactome-links.csv", 'w', encoding='utf-8', newline='') as file:
        #     writer = csv.writer(file)
        #     writer.writerows(rows)

# reactome_to_data_distillery()


def reactome_reactions():
    file = open("../data/reactome/dd_reactions-go.csv", 'w', encoding='utf-8', newline='')
    writer = csv.writer(file)

    with driver.session() as session:
        cql = """match (r:Reaction)-[:compartment|goBiologicalProcess]->(db: GO_Term {databaseName: "GO"}) return r, db"""
        res = session.run(cql)
        ios = [record for record in res.data()]
        for pe in ios:
            row = [pe['r']['stId'], pe['r']['displayName'], pe['r']['name'], pe['db']['dbId'], pe['db']['displayName'], pe['db']['url']]
            writer.writerow(row)


reactome_reactions()