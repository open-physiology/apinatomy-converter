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

	# neo4j_db.clear_graph()

	# Connected structures
	nodes = mysql_db.query_connected_structures()
	neo4j_db.create_nodes(nodes, "Structure")

	# Microcirculation links
	links = mysql_db.query_micro()
	neo4j_db.create_links(links)

	# All rows from branching order table
	branches = mysql_db.query_branches_all()
	neo4j_db.create_branches_all(branches)

	# Additional arterial connections
	arterial = mysql_db.query_arterial_connections()
	neo4j_db.create_links(arterial)

	# Additional venous connections
	venous = mysql_db.query_venous_connections()
	neo4j_db.create_links(venous)


