import os
from model.neo4j_connector import NEO4JConnector
from model.mysql_connector import MySQLConnector

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
	# DB to store data (Neo4j apinatomy)
	neo4j_address = os.environ.get('APINATOMY_NEO4J')
	neo4j_pwd = os.environ.get('APINATOMY_NEO4J_PASSWORD')
	neo4j_db = NEO4JConnector(neo4j_address, "neo4j", neo4j_pwd)

	# DB to read data (MySQL apinatomy)
	mysql_user = os.environ.get('APINATOMY_MYSQL_USER')
	mysql_pwd = os.environ.get('APINATOMY_MYSQL_PASSWORD')

	# Connect to apinatomy
	mysql_db = MySQLConnector("db8h1l9gpgullg", mysql_user, mysql_pwd, "es35.siteground.eu", "3306")

	#neo4j_db.clear_graph()

	# Microcirculation organs and vessels
	organs = mysql_db.query_organs()
	vessels = mysql_db.query_vessels()
	neo4j_db.create_nodes(organs, "Organ")
	neo4j_db.create_nodes(vessels, "Vessel")

	# Microcirculation links
	links = mysql_db.query_micro()
	neo4j_db.create_links(links)

	# Microcirculation connections in branching order
	branches = mysql_db.query_branches()
	neo4j_db.create_branches_all(branches)

	# All data (structures + branching_order)
	# nodes = mysql_db.query_structures()
	# neo4j_db.create_nodes(nodes, "Node")
	# branches = mysql_db.query_branches_all()
	# neo4j_db.create_branches_all(branches)

