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
        cql_delete_relationships = "MATCH (a) -[r] -> () DELETE a, r"
        cql_delete_nodes = "MATCH (a) DELETE a"
        session.run(cql_delete_relationships)
        session.run(cql_delete_nodes)

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
        cql_match = """MATCH (a:Organ), (b:Vessel) WHERE a.fmaID = "{0}" AND b.fmaID = "{1}" """
        cql_create = """CREATE (a)-[:Microcirculation {type: $rel_type}]->(b)"""
        for item in items:
            print("# rels before adding: %d", self.query_rel_count())
            query = cql_match.format(item["organFMA"], item["vesselFMA"]) + cql_create
            session.run(query, rel_type = item["type"])
            print("# rels after adding: %d", self.query_rel_count())

    @staticmethod
    def create_node(item, cls_name: str, session: Session):
        cql_create = """CREATE (:{0} {1})"""
        q = cql_create.format(cls_name, serialize(item))
        session.run(q)

    def create_branches_all(self, branches, session: Session = None):
        if session is None:
            session = self.driver.session()
        print("# relationships before adding: %d", self.query_rel_count())
        prev_s = None
        prev_m = None
        for branch in branches:
            print(branch)
            s_id = branch["source"]
            t_id = branch["target"]
            # s_name = branch["sourceName"]
            # t_name = branch["targetName"]
            order = branch["order"]

            if prev_s and prev_s==s_id:
                s = prev_m
            else:
                s = self.query_node(s_id)
                if s is None:
                    # s = {"nodeID": s_id, "fmaID": s_id, "name": s_name}
                    s = {"nodeID": s_id, "fmaID": s_id}
                    self.create_node(s, "Node", session)

            t = self.query_node(t_id)
            if t is None:
                # t = {"nodeID": t_id, "fmaID": t_id, "name": t_name}
                t = {"nodeID": t_id, "fmaID": t_id}
                self.create_node(t, "Node", session)

            m_id = s_id + "_" + t_id + "_" + str(order)
            m = {"nodeID": m_id}
            self.create_node(m, "Node", session)

            q = """MATCH (a),(b) WHERE a.nodeID=$s_id AND b.nodeID=$t_id CREATE(a)-[:Connects]->(b)"""
            session.run(q.format(order), s_id=s["nodeID"], t_id=m_id)
            session.run(q.format(order), s_id=m_id, t_id=t_id)

            prev_s = s_id
            prev_m = m

        print("# relationships after adding: %d", self.query_rel_count())

        # To get a path from a given resource
        # MATCH p = ({fmaID:"3802"})-[*]-() RETURN nodes(p)