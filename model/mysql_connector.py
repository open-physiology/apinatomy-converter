import mysql.connector


class MySQLConnector:

    def __init__(self, database: str, user: str, password: str, host: str, port: str):
        self.driver = mysql.connector.connect(user=user, password=password, database=database, host=host, port=port)

    # Retrieve organs from microcirculations
    def query_organs(self):
        organs = []
        cursor = self.driver.cursor()
        query = ('SELECT DISTINCT microcirculations.ORGAN, structures.STRUCTURE_NAME FROM microcirculations, '
                 'structures where microcirculations.ORGAN = structures.FMA_ID')
        cursor.execute(query)
        for item in cursor:
            obj = {"nodeID": item[0], "fmaID": item[0], "name": item[1]}
            organs.append(obj)
        cursor.close()
        return organs

    # Retrieve vessels from microcirculations
    def query_vessels(self):
        vessels = []
        cursor = self.driver.cursor()
        query = ('SELECT DISTINCT microcirculations.VESSEL, structures.STRUCTURE_NAME FROM microcirculations, '
                 'structures where microcirculations.VESSEL = structures.FMA_ID')
        cursor.execute(query)
        for item in cursor:
            obj = {"nodeID": item[0], "fmaID": item[0], "name": item[1]}
            vessels.append(obj)
        cursor.close()
        return vessels

    def query_micro(self):
        links = []
        cursor = self.driver.cursor()
        query = ('SELECT ORGAN, VESSEL, TYPE FROM microcirculations')
        cursor.execute(query)
        for item in cursor:
            obj = {"source": item[0], "target": item[1], "type": item[2]}
            # print(obj)
            links.append(obj)
        cursor.close()
        return links

    # All body structure definitions
    def query_structures(self):
        structures = []
        cursor = self.driver.cursor()
        query = ('SELECT DISTINCT FMA_ID, STRUCTURE_NAME FROM structures')
        cursor.execute(query)
        for item in cursor:
            obj = {"nodeID": item[0], "fmaID": item[0], "name": item[1]}
            structures.append(obj)
        cursor.close()
        return structures

    # Body structure by fmaID
    def query_structure_by_fma_id(self, fma_id):
        obj = None
        cursor = self.driver.cursor()
        query = ('SELECT FMA_ID, STRUCTURE_NAME FROM structures where FMA_ID=$fma_id')
        cursor.execute(query, fma_id=fma_id)
        if len(cursor) > 0:
            item = cursor[0]
            obj = {"nodeID": item[0], "fmaID": item[0], "name": item[1]}
        cursor.close()
        return obj

    def query_connected_structures(self):
        cursor = self.driver.cursor()
        query = """SELECT DISTINCT v.node, s.STRUCTURE_NAME FROM (
SELECT vn.VESSEL_FROM AS node FROM venous_network vn
UNION SELECT vn.VESSEL_TO AS node FROM venous_network vn
UNION SELECT an.VESSEL_FROM AS node FROM arterial_network an
UNION SELECT an.VESSEL_TO AS node FROM arterial_network an
UNION SELECT br.MAIN_VESSEL AS node FROM branching_order br
UNION SELECT br.BRANCH as node FROM branching_order br
UNION SELECT mc.ORGAN FROM microcirculations mc
UNION SELECT mc.VESSEL FROM microcirculations mc) v, structures s where v.node = s.FMA_ID"""
        cursor.execute(query)
        nodes = []
        for item in cursor:
            obj = {"nodeID": item[0], "fmaID": item[0], "name": item[1]}
            nodes.append(obj)
        cursor.close()
        return nodes

    # Venous connections which are not in branching order
    def query_venous_connections(self):
        branches = []
        cursor = self.driver.cursor()
        query =  """SELECT DISTINCT vn.VESSEL_FROM, vn.VESSEL_TO, 'VEN' as type from venous_network vn
LEFT JOIN branching_order br ON vn.VESSEL_FROM = br.MAIN_VESSEL and vn.VESSEL_TO = br.BRANCH
WHERE br.MAIN_VESSEL is NULL"""
        cursor.execute(query)
        for item in cursor:
            obj = {"source": str(item[0]), "target": str(item[1]), "type": item[2]}
            branches.append(obj)
        cursor.close()
        return branches

    # Arterial connections which are not in branching order
    def query_arterial_connections(self):
        branches = []
        cursor = self.driver.cursor()
        query = """SELECT DISTINCT an.VESSEL_FROM, an.VESSEL_TO, 'ART' as type from arterial_network an
LEFT JOIN branching_order br ON an.VESSEL_FROM = br.MAIN_VESSEL and an.VESSEL_TO = br.BRANCH
WHERE br.MAIN_VESSEL is NULL"""
        cursor.execute(query)
        for item in cursor:
            obj = {"source": str(item[0]), "target": str(item[1]), "type": item[2]}
            branches.append(obj)
        cursor.close()
        return branches

    # All connections
    def query_branches_all(self):
        branches = []
        cursor = self.driver.cursor()
        query = """SELECT DISTINCT MAIN_VESSEL, BRANCH, SEQUENCE, TYPE FROM branching_order br,
(SELECT DISTINCT FMA_ID, TYPE from vascular_segments) vs
where vs.FMA_ID = br.MAIN_VESSEL 
order by MAIN_VESSEL, SEQUENCE"""
        cursor.execute(query)
        for item in cursor:
            obj = {"source": str(item[0]), "target": str(item[1]), "order": item[2], "type": item[3]}
            branches.append(obj)
        cursor.close()
        return branches

