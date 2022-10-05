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
            obj = {"organFMA": item[0], "vesselFMA": item[1], "type": item[2]}
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
    def query_structure(self, fma_id):
        obj = None
        cursor = self.driver.cursor()
        query = ('SELECT FMA_ID, STRUCTURE_NAME FROM structures where FMA_ID=$fma_id')
        cursor.execute(query, fma_id=fma_id)
        if len(cursor) > 0:
            item = cursor[0]
            obj = {"nodeID": item[0], "fmaID": item[0], "name": item[1]}
        cursor.close()
        return obj

    # Connections which are in microcirculations
    def query_branches(self):
        branches = []
        cursor = self.driver.cursor()
        query = ('SELECT DISTINCT MAIN_VESSEL, BRANCH, SEQUENCE FROM branching_order, microcirculations m1, '
                 'microcirculations m2 where MAIN_VESSEL = m1.VESSEL and BRANCH = m2.VESSEL order by MAIN_VESSEL, SEQUENCE')
        cursor.execute(query)
        for item in cursor:
            obj = {"source": str(item[0]), "target": str(item[1]), "order": item[2]}
            branches.append(obj)
        cursor.close()
        return branches

    # All connections
    def query_branches_all(self):
        branches = []
        cursor = self.driver.cursor()
        query = ('SELECT DISTINCT MAIN_VESSEL, BRANCH, SEQUENCE, s1.STRUCTURE_NAME, s2.STRUCTURE_NAME FROM '
                 'branching_order, structures s1, structures s2 where MAIN_VESSEL = s1.FMA_ID and BRANCH = s2.FMA_ID '
                 'order by MAIN_VESSEL, SEQUENCE')
        cursor.execute(query)
        for item in cursor:
            obj = {"source": str(item[0]), "target": str(item[1]), "order": item[2],
                   "sourceName": item[3], "targetName": item[4]}
            branches.append(obj)
        cursor.close()
        return branches