import mysql.connector


class MySQLConnector:

    def __init__(self, database: str, user: str, password: str, host: str, port: str):
        self.driver = mysql.connector.connect(user=user, password=password, database=database, host=host, port=port)

    def create_fma_table(self, columns):
        cursor = self.driver.cursor()
        cols = ['{} text'.format(x.replace(" ", "_").replace("-", "_")
                                 .replace('partition', 'partition_0').replace('union', 'union_0')) for x in columns]
        col_str = ', '.join(cols)
        query = """CREATE TABLE fma ({})""".format(col_str)
        print(query)
        cursor.execute(query)

    def insert_fma_record(self, col_str, row):
        cursor = self.driver.cursor()
        values = ["'{}'".format(x) if '"' in x else '"{}"'.format(x) for x in row]
        query = "INSERT INTO fma {} VALUES ({})".format(col_str, ','.join(values))
        cursor.execute(query)
        self.driver.commit()

    def query_master_vascular_nodes(self):
        cursor = self.driver.cursor()
        query = """SELECT DISTINCT node_id, fma_id, name FROM 
            (SELECT MAIN_VESSEL as node_id, vl1originalID as fma_id, MAIN_NAME as name FROM master_vascular mv
            UNION SELECT BRANCH as node_id, vl2originalID as fma_id, BRANCH_NAME AS fma_id FROM master_vascular mv) v"""
        cursor.execute(query)
        nodes = []
        for item in cursor:
            obj = {"nodeID": item[0], "fmaID": item[1], "name": item[2]}
            nodes.append(obj)
        return nodes

    def query_master_vascular_nodes_map(self):
        cursor = self.driver.cursor()
        query = """SELECT DISTINCT node_id, fma_id, name FROM 
            (SELECT MAIN_VESSEL as node_id, vl1originalID as fma_id, MAIN_NAME as name FROM master_vascular mv
            UNION SELECT BRANCH as node_id, vl2originalID as fma_id, BRANCH_NAME AS fma_id FROM master_vascular mv) v"""
        cursor.execute(query)
        nodes = {}
        for item in cursor:
            obj = {"nodeID": str(item[0]), "fmaID": str(item[1]), "name": item[2]}
            nodes[obj["nodeID"]] = obj
        return nodes

    def query_lyph_templates(self):
        cursor = self.driver.cursor()
        query = """SELECT VESSEL_FMA_ID as fmaID, lyph_template_ID as lyphTemplate, node_colour AS color FROM `master_vascular_lyph_templates`"""
        cursor.execute(query)
        nodes = {}
        for item in cursor:
            if len(str(item[1])) > 0:
                obj = {"fmaID": item[0], "lyphTemplate": item[1], "color": item[2]}
            nodes[item[0]] = obj
        return nodes

    def query_master_vascular_node_colors(self):
        cursor = self.driver.cursor()
        query = """SELECT DISTINCT MAIN_VESSEL as node_id, colour FROM master_vascular"""
        cursor.execute(query)
        nodes = []
        for item in cursor:
            obj = {"nodeID": str(item[0]), "color": item[1]}
            nodes.append(obj)
        return nodes

    def query_master_vascular_edges(self):
        branches = []
        cursor = self.driver.cursor()

        # Unordered branches

        # query = """SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, CASE WHEN sequence = 0 THEN 1000 ELSE sequence END as
        #     sequence, vl1originalID, vl2originalID, colour FROM master_vascular WHERE MAIN_VESSEL in
        #     (SELECT DISTINCT BRANCH FROM master_vascular WHERE vl1originalID=50040 OR vl1originalID=50039)
        #     order by vl1originalID, MAIN_VESSEL, sequence"""

        # query = """SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, CASE WHEN sequence = 0 THEN 1000 ELSE sequence END as
        # sequence, vl1originalID, vl2originalID, colour FROM master_vascular WHERE vl1originalID=50040 OR vl1originalID=50039 order by vl1originalID, MAIN_VESSEL, sequence"""

        # query = """SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, CASE WHEN sequence = 0 THEN 1000 ELSE sequence END as
        # sequence, vl1originalID, vl2originalID, colour FROM `master_vascular` WHERE vl1originalvesselname='Aorta' and MAIN_VESSEL='50010931' order by MAIN_VESSEL, sequence"""

        # No unordered branches

        # query = """SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, sequence, vl1originalID, vl2originalID, colour FROM master_vascular
        # WHERE MAIN_VESSEL in (SELECT DISTINCT BRANCH FROM master_vascular WHERE (vl1originalID=50040 OR vl1originalID=50039) and sequence > 0) and sequence > 0
        #     order by vl1originalID, MAIN_VESSEL, sequence"""
        #
        # query = """SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, sequence, vl1originalID, vl2originalID, colour FROM master_vascular
        # WHERE (vl1originalID=50040 OR vl1originalID=50039) and sequence > 0 order by vl1originalID, MAIN_VESSEL, sequence"""
        #
        # query = """SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, sequence, vl1originalID, vl2originalID, colour FROM `master_vascular`
        # WHERE vl1originalvesselname='Aorta' and MAIN_VESSEL='50010931' and sequence > 0 order by MAIN_VESSEL, sequence"""

        # Everything without unordered branches
        # query = """
        #     SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, sequence, vl1originalID, vl2originalID, lyph_template_ID, node_colour from (
        #         SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, sequence, vl1originalID, vl2originalID FROM master_vascular
        #         WHERE MAIN_VESSEL in (SELECT DISTINCT BRANCH FROM master_vascular WHERE (vl1originalID=50040 OR vl1originalID=50039) and sequence > 0) and sequence > 0
        #         union
        #         SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, sequence, vl1originalID, vl2originalID FROM master_vascular
        #                 WHERE (vl1originalID=50040 OR vl1originalID=50039) and sequence > 0
        #         union
        #         SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, sequence, vl1originalID, vl2originalID FROM `master_vascular`
        #                 WHERE vl1originalvesselname='Aorta' and MAIN_VESSEL='50010931' and sequence > 0) as v
        #         LEFT OUTER JOIN `master_vascular_lyph_templates` as lt ON v.vl2originalID = lt.VESSEL_FMA_ID
        #         order by vl1originalID, MAIN_VESSEL, sequence
        # """
        # query = """SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, sequence, vl1originalID, vl2originalID, lyph_template_ID, node_colour from (
        #         SELECT * FROM master_vascular WHERE MAIN_VESSEL in
        #         (SELECT DISTINCT BRANCH FROM master_vascular, master_vascular_lyph_templates WHERE vl2originalID = VESSEL_FMA_ID and sequence > 0 and include)
        #         and sequence > 0) as v
        #         LEFT OUTER JOIN `master_vascular_lyph_templates` as lt ON v.vl2originalID = lt.VESSEL_FMA_ID
        #         order by vl1originalID, MAIN_VESSEL, sequence
        # """
        query = """SELECT MAIN_VESSEL, BRANCH, vl2trunkname, vl2trunkID, vessel_type, 
                     CASE WHEN sequence = 0 THEN 1000 ELSE sequence END as sequence, 
                     vl1originalID, vl2originalID, lyph_template_ID, node_colour from (
                     SELECT * FROM master_vascular WHERE MAIN_VESSEL in 
                     (SELECT DISTINCT BRANCH FROM master_vascular, master_vascular_lyph_templates WHERE vl2originalID = VESSEL_FMA_ID and include)) as v
                     LEFT OUTER JOIN `master_vascular_lyph_templates` as lt ON v.vl1originalID = lt.VESSEL_FMA_ID  
                     order by vl1originalID, sequence
             """
        cursor.execute(query)
        prev = None
        count = 1
        for item in cursor:
            source = str(item[0])
            target = str(item[1])
            if source == prev:
                count += 1
            else:
                for b in branches:
                    if b["source"] == prev:
                        b["total"] = count
                count = 1
            obj = {"source": str(source), "target": str(target), "label": item[2], 'id': item[3], 'type': item[4],
                   'order': item[5], 'source_fma': str(item[6]), 'target_fma': str(item[7]), 'lyph_template': item[8], 'color': item[9]}
            branches.append(obj)
            prev = source
        for b in branches:
            if b["source"] == prev:
                b["total"] = count
        cursor.close()
        return branches

