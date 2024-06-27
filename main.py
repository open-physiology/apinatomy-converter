# Convert vascular data from MySQL to Neo4J
import pandas as pd
import json
from model.neo4j_connector import NEO4JConnector
from model.mysql_connector_master import MySQLConnector

# DB to read data (MySQL apinatomy)
login = json.load(open('./credentials/api_mysql_db_nk.json', 'r'))
db_name, db_user, db_pwd, db_server, db_port = (login.values())
mysql_db = MySQLConnector(db_name, db_user, db_pwd, db_server, db_port)

# DB to store data (Neo4j Apinatomy vascular)
# login = json.load(open('credentials/api_neo4j_test_db.json', 'r'))
# neo4j_address, neo4j_pwd = (login.values())
# neo4j_db = NEO4JConnector(neo4j_address, "neo4j", neo4j_pwd)


def convert_vascular_data():
	# neo4j_db.clear_relationships()
	# neo4j_db.detach_delete_nodes("Connector")

	# Connected structures
	# nodes = mysql_db.query_connected_structures()
	# neo4j_db.merge_nodes(nodes, "Structure")

	# nodes = mysql_db.query_network_map()
	# branches = mysql_db.query_network()
	# mysql_db.create_lyphs(branches)

	# filtered = []
	# found = False
	# for b in branches:
	# 	if found:
	# 		filtered.append(b)
	# 	if b['source'] == '50010941' and b['target'] == '50010940':
	# 		found = True
	# print(len(filtered))
	# neo4j_db.create_network(filtered)
	# neo4j_db.update_network(filtered)

	# All rows from branching order table
	# branches = mysql_db.query_branches_all()
	# neo4j_db.create_network(branches)

	# # Additional arterial connections
	# arterial = mysql_db.query_arterial_connections()
	# neo4j_db.create_links(arterial)
	#
	# # Additional venous connections
	# venous = mysql_db.query_venous_connections()
	# neo4j_db.create_links(venous)

	# Microcirculation links
	# links = mysql_db.query_micro()
	# neo4j_db.create_microcirculations(links)

	# To clean, remove nodes labelled as 'Vascular' and relationships of type 'vascular'
	# nodes = mysql_db.query_vascular_structures()
	# neo4j_db.create_nodes(nodes, "Vascular")

	# segments = mysql_db.query_vascular_segments()
	# neo4j_db.create_links_with_labels(segments)
	pass


def convert_master_vascular():
	# Clear DB
	# neo4j_db.clear_relationships()
	# neo4j_db.detach_delete_nodes("Structure")
	# neo4j_db.detach_delete_nodes("Connector")

	# Connected structures
	# nodes = mysql_db.query_master_vascular_nodes()
	# neo4j_db.merge_nodes(nodes, "Structure")

	branches = mysql_db.query_master_vascular_edges()
	# neo4j_db.create_network(branches)
	neo4j_db.update_network(branches)


def generate_master_vascular_chains():
	# Get source data
	node_map = mysql_db.query_master_vascular_nodes_map()
	branches = mysql_db.query_master_vascular_edges()

	def create_local_excel(file_path):
		writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
		# main
		df_main = pd.DataFrame(columns=["id", "name", "author", "namespace", "description", "imports"])
		df_main.loc[len(df_main.index)] = ["vascular-vessels", "Vascular vessels", "Natallia Kokash",
										   "vascular", "Generated vascular chains from MySQL data",
										   "https://raw.githubusercontent.com/open-physiology/apinatomy-models/master/models/wbrcm/source/wbrcm.json"]
		df_main.to_excel(writer, sheet_name='main', index=False)
		df_lyphs.to_excel(writer, sheet_name='lyphs', index=False)
		df_nodes.to_excel(writer, sheet_name='nodes', index=False)
		df_links.to_excel(writer, sheet_name='links', index=False)
		df_chains.to_excel(writer, sheet_name='chains', index=False)
		df_groups.to_excel(writer, sheet_name='groups', index=False)
		# local conventions
		local_conventions = {
			"prefix": ["UBERON", "CHEBI", "FMA", "GO", "ILX", "NLX", "SAO", "PMID", "EMAPA", "CL", "NCBITaxon", "wbkg", "too"],
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
		writer.close()

	# Create ApiNATOMY tables
	NODE_COLUMNS = ["id", "name", "ontologyTerms"]
	LINK_COLUMNS = ["id", "name", "conveyingLyph", "source", "target", "ontologyTerms", "color"]
	LYPH_COLUMNS = ["id", "name", "supertype", "ontologyTerms"]
	CHAIN_COLUMNS = ["id", "name", "housingLyphs", "lyphs", "levels"]
	GROUP_COLUMNS = ["id", "name", "lyphs", "links", "nodes"]

	df_nodes = pd.DataFrame([], columns=NODE_COLUMNS)
	df_lyphs = pd.DataFrame([], columns=LYPH_COLUMNS)
	df_links = pd.DataFrame([], columns=LINK_COLUMNS)
	df_chains = pd.DataFrame([], columns=CHAIN_COLUMNS)
	df_groups = pd.DataFrame([], columns=GROUP_COLUMNS)

	# Fill dataframes with generated chains
	def create_chains():
		def get_label(source_name, target_name, order):
			return "Segment " + order + " from " + source_name + " to " + target_name

		# Create end nodes
		used_node_map = {}
		for branch in branches:
			s_id = branch["source"]
			t_id = branch["target"]

			def create_node(x_id):
				if x_id not in used_node_map:
					x = node_map[x_id]
					used_node_map[x["nodeID"]] = {
						"id": x["nodeID"], "name": x["name"], "ontologyTerms": "FMA:"+x["fmaID"]}

			create_node(s_id)
			create_node(t_id)

		# By dumping nodes here we avoid declaration of connector nodes, they will be generated
		for key in used_node_map:
			df_nodes.loc[len(df_nodes.index)] = used_node_map[key].values()

		# Create links
		prev_s_id = None
		prev_m_id = None
		source_name = "origin"
		lyph_template = None

		lyph_map = {}
		group = None
		g_lyphs = []
		g_links = []
		g_nodes = []

		for branch in branches:
			print("Processing branch:", branch)
			s_id = branch["source"]
			t_id = branch["target"]
			order = str(branch["order"])
			total = branch["total"] if "total" in branch else 1
			color = branch["color"]

			if prev_s_id == s_id:
				s = used_node_map[prev_m_id]
			else:
				s = used_node_map[s_id]
				source_name = s["name"]
				lyph_template = "wbkg:" + branch["lyph_template"] if branch["lyph_template"] else ""
				if group:
					group["lyphs"] = ",".join(g_lyphs)
					group["links"] = ",".join(g_links)
					group["nodes"] = ",".join(g_nodes)
					df_groups.loc[len(df_groups.index)] = group.values()
				group = {"id": "g_" + s_id, "name": "Group for " + source_name}
				g_links = []
				g_lyphs = []
				g_nodes = []

			t = used_node_map[t_id]

			# Create links
			if total == 1:
				lnk_id = "lnk-" + s["id"]+"_"+t_id
				lnk = {
					"id": lnk_id, "name": "", "conveyingLyph": lyph_template, "source": s["id"], "target": t_id,
					"ontologyTerms": "FMA:"+t["ontologyTerms"], "color": color
				}
				df_links.loc[len(df_links.index)] = lnk.values()
				group = None
			else:
				m_id = s_id + "_" + t_id + "_" + order
				m_label = get_label(source_name, t["name"], order)

				# Create connector nodes
				used_node_map[m_id] = {"id": m_id, "name": m_label, "ontologyTerms": ""}

				lnk1_id = "lnk-" + s["id"] + "_" + m_id
				lnk1 = {
					"id": lnk1_id, "name": "", "conveyingLyph": lyph_template, "source": s["id"], "target":m_id,
					"ontologyTerms": "FMA:"+branch['source_fma'], "color": color
				}

				lyph_id = "lyph-" + branch["target_fma"]
				if lyph_id not in lyph_map:
					lyph_map[lyph_id] = {
						"id": lyph_id,
						"name": t["name"],
						"supertype": "",
						"ontologyTerms": "FMA:" + branch["target_fma"]
					}

				lnk2_id = "lnk-" + m_id + "_" + t_id
				lnk2 = {
					"id": lnk2_id, "name": "", "conveyingLyph": lyph_id, "source": m_id, "target": t_id,
					"ontologyTerms": "FMA:"+branch["target_fma"], "color": color
				}

				df_links.loc[len(df_links.index)] = lnk1.values()
				df_links.loc[len(df_links.index)] = lnk2.values()

				if group:
					g_links.append(lnk1_id)
					g_links.append(lnk2_id)
					g_lyphs.append(lyph_id)
					g_nodes.append(lnk1["source"])
					g_nodes.append(lnk1["target"])
					g_nodes.append(lnk2["source"])
					g_nodes.append(lnk2["target"])

				prev_m_id = m_id

			prev_s_id = s_id

		for key in lyph_map:
			df_lyphs.loc[len(df_lyphs.index)] = lyph_map[key].values()

	create_chains()
	create_local_excel("data/vascular.xlsx")


if __name__ == '__main__':
	# convert_master_vascular()
	generate_master_vascular_chains()


# Graph visualization
# https://neo4j.com/labs/apoc/4.3/export/graphml/
# https://www.yworks.com/yed-live/



