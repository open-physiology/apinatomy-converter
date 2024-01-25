from neo4j import GraphDatabase

neo4j_uri = "neo4j+ssc://ffc8f87e.databases.neo4j.io"
neo4j_pwd = "zJ_lVZeI29XAdLIVMZU0PMC_B-k961i0UpB2tVJeJqY"

driver = GraphDatabase.driver(neo4j_uri, auth=("neo4j", neo4j_pwd))


def expand_complex(session, pe_stId):
    cql_get_components = """
                        match  path = (n:PhysicalEntity)-[r2:hasComponent*]->(m) where n.stIdVersion=$pe_stId
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
        print('\t\t', c['leaf']["stIdVersion"], c['leaf']["schemaClass"], c['leaf']["displayName"],
              c['c']['url'])


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
                        match path = (n:PhysicalEntity)-[r2:hasComponent*]->(m) where n.stIdVersion=$pe_stId
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
                    if pe['pe']['schemaClass'] == 'Complex':
                        pe_stId = pe['pe']["stIdVersion"]
                        print(f"\t\tcomplex {pe['pe']['schemaClass']}")
                        complex = expand_complex_uniprot(session, pe_stId)
                        for c in complex:
                            c1 = [r_stId, r_displayName] + c
                            rows.append(c1)
        import csv
        with open("./data/sodium.csv", 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)


get_sodium_reactions()


