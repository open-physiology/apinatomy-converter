# author: Natallia Kokash, natallia.kokash@gmail.com

from neo4j import GraphDatabase, Session

from typing import List


def serialize(obj):
    return "{" + ', '.join('{0}: "{1}"'.format(key, value) for (key, value) in obj.items()) + "}"


class NEO4JConnector:

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    # Delete
    def clear_graph(self, session: Session = None):
        if session is None:
            session = self.driver.session()
        self.clear_relationships(session)
        cql_delete_nodes = "MATCH (a) DELETE a"
        session.run(cql_delete_nodes)

    def clear_relationships(self, session: Session = None):
        if session is None:
            session = self.driver.session()
        print("# relationships before deleting: %d", self.query_rel_count())
        cql_delete_relationships = "MATCH (a) -[r] -> () DELETE r"
        session.run(cql_delete_relationships)
        print("# relationships after deleting: %d", self.query_rel_count())

    def clear_node_cls(self, cls_name, session: Session = None):
        if session is None:
            session = self.driver.session()
        print("# nodes before deleting: %d", self.query_node_count())
        cql_delete_nodes = "MATCH (a: {0}) DELETE a".format(cls_name)
        session.run(cql_delete_nodes)
        print("# nodes after deleting: %d", self.query_node_count())

    def delete_node(self, node_uuid: str, session: Session = None):
        if session is None:
            session = self.driver.session()
        cql_delete_nodes = "MATCH (a {UUID: $node_uuid}) DETACH DELETE a"
        session.run(cql_delete_nodes, node_uuid=node_uuid)

    def detach_delete_nodes(self, node_class: str, session: Session = None):
        if session is None:
            session = self.driver.session()
        cql_delete_nodes = "MATCH (a: {node_class}) DETACH DELETE a".format(node_class=node_class)
        session.run(cql_delete_nodes)

    def delete_empty_nodes(self, node_class: str, session: Session = None):
        if session is None:
            session = self.driver.session()
        cql_delete_nodes = "MATCH (a: {node_class}) WHERE size((a)--())=0 DELETE a".format(node_class=node_class)
        session.run(cql_delete_nodes)

    # Query

    def query_node_count(self) -> int:
        cql_count = "MATCH (a) return count(a) as node_count"
        with self.driver.session() as session:
            res = session.run(cql_count)
            entries = [record for record in res.data()]
            return entries[0]['node_count']

    def query_rel_count(self) -> int:
        cql_count = "MATCH ()-->() RETURN COUNT(*) AS rel_count"
        with self.driver.session() as session:
            res = session.run(cql_count)
            entries = [record for record in res.data()]
            return entries[0]['rel_count']

    def query_node(self, node_id, session=None) -> List[str]:
        if session is None:
            session = self.driver.session()
        cql_node = "MATCH (a) where a.nodeID=$node_id return a"
        nodes = session.run(cql_node, node_id=node_id)
        db_items = [record for record in nodes.data()]
        if len(db_items) > 0:
            return db_items[0]['a']

    # Create

    def create_nodes(self, items, cls_name: str, session: Session = None):
        if session is None:
            session = self.driver.session()
        print("# nodes before adding: %d", self.query_node_count())
        for item in items:
            self.create_node(item, cls_name, session)
        print("# nodes after adding: %d", self.query_node_count())

    def create_links(self, items, session: Session = None):
        if session is None:
            session = self.driver.session()
        cql_match = """MATCH (a), (b) WHERE a.fmaID = "{0}" AND b.fmaID = "{1}" """
        cql_create = """CREATE (a)-[:Connects {type: $rel_type}]->(b)"""
        print("# rels before adding: %d", self.query_rel_count())
        for item in items:
            query = cql_match.format(item["source"], item["target"]) + cql_create
            session.run(query, rel_type=item["type"])
        print("# rels after adding: %d", self.query_rel_count())

    def create_microcirculations(self, items, session: Session = None):
        if session is None:
            session = self.driver.session()
        cql_match = """MATCH (a), (b) WHERE a.fmaID = "{0}" AND b.nodeID = "{1}" """
        cql_create = """CREATE (a)-[:{0}]->(b)"""
        cql_create_reverse = """CREATE (b)-[:{0}]->(a)"""

        print("# rels before adding: %d", self.query_rel_count())
        for item in items:
            mc_id = "mc_" + str(item["source"]) + "_" + str(item["target"])
            mc_type = item["type"]
            self.create_node({"nodeID": mc_id, "type": mc_type}, "Microcirculation", session)
            q_organ_to_mc = cql_match.format(item["source"], mc_id)
            q_vessel_to_mc = cql_match.format(item["target"], mc_id)
            if mc_type == "VEN":
                q = q_organ_to_mc + cql_create.format("Supplies")
                # print(q)
                session.run(q)
                q = q_vessel_to_mc + cql_create_reverse.format("Drains")
                # print(q)
                session.run(q)
            elif mc_type == "ART":
                q = q_vessel_to_mc + cql_create.format("Supplies")
                # print(q)
                session.run(q)
                q = q_organ_to_mc + cql_create_reverse.format("Drains")
                # print(q)
                session.run(q)

        print("# rels after adding: %d", self.query_rel_count())


    @staticmethod
    def create_node(item, cls_name: str, session: Session):
        cql_create = """CREATE (:{0} {1})"""
        q = cql_create.format(cls_name, serialize(item))
        session.run(q)

    def create_branches_all(self, branches, session: Session = None):
        if session is None:
            session = self.driver.session()

        def get_label(type, main_name, prev_name, branch_name):
            segment = "Arterial" if type == 1 else "Venous"
            return segment + " segment of " + main_name + " from " + prev_name + " to " + branch_name

        print("# relationships before adding: %d", self.query_rel_count())
        main_name = None
        prev_name = "origin"
        prev_s = None
        prev_m = None
        for branch in branches:
            print(branch)
            s_id = branch["source"]
            t_id = branch["target"]
            order = branch["order"]
            type = branch["type"]

            if prev_s and prev_s == s_id:
                s = prev_m
            else:
                s = self.query_node(s_id)
                if s is None:
                    print("Failed to find resource: ", s_id)
                    return
                main_name = s["name"]
                prev_name = "origin"

            t = self.query_node(t_id)
            if t is None:
                print("Failed to find resource: ", t_id)
                return
            branch_name = t["name"]

            label = get_label(type, main_name, prev_name, branch_name)
            m_id = s_id + "_" + t_id + "_" + str(order)
            m = {"nodeID": m_id, "name": label, "type": 'ART' if type == 1 else 'VEN'}
            self.create_node(m, "Connector", session)
            prev_name = branch_name

            q = """MATCH (a),(b) WHERE a.nodeID=$s_id AND b.nodeID=$t_id CREATE(a)-[:Connects]->(b)"""
            session.run(q.format(order), s_id=s["nodeID"], t_id=m_id)
            session.run(q.format(order), s_id=m_id, t_id=t_id)

            prev_s = s_id
            prev_m = m

        print("# relationships after adding: %d", self.query_rel_count())

        # To get a path between two resources
        # MATCH p = ({fmaID:"7101"})-[*]->({fmaID:"66363"}) RETURN nodes(p)

        # To get a path between two resources with attached nodes representing FMA structures:
        # MATCH p = ({fmaID:"7101"})-[*]->({fmaID:"66363"}) with  nodes(p)
        # as path unwind path as m match (m:Connector)-[]->(n) return path, n

