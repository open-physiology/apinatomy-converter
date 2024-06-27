import matplotlib
import matplotlib.pyplot as plt
import networkx as nx
import csv

matplotlib.use('TkAgg')


# Graph includes nodes of type Reaction (blue), GO (red), and Uniprot/ChEBI (green)
# Edges are of types hasGOTerm (red) and hasInput (green)
def plot_reactome_graph(edges, nodes):
    # print(nodes)
    # print(edges)

    G = nx.DiGraph()
    G.add_edges_from(edges)

    has_go_edges = [edge for edge in G.edges() if nodes[edge[1]]["type"] == "GO"]
    has_input_edges = [edge for edge in G.edges() if nodes[edge[1]]["type"] != "GO"]

    pos = nx.circular_layout(G)
    pos_labels = {}
    y_off = 0.03  # offset on the y axis
    for k, v in pos.items():
        pos_labels[k] = (v[0], v[1] + y_off)

    # node_size = [len(v) * 500 for v in G.nodes()] #resize nodes to fit labels
    reactions = [node for node in G.nodes() if nodes[node]["type"] == "Reaction"]
    go_terms = [node for node in G.nodes() if nodes[node]["type"] == "GO"]
    other = [node for node in G.nodes() if nodes[node]["type"] == "Uniprot/ChEBI"]

    nx.draw_networkx_nodes(G, pos, nodelist=reactions, node_size=100, cmap=plt.get_cmap('rainbow'), node_color='b',
                           label="Reaction")
    nx.draw_networkx_nodes(G, pos, nodelist=go_terms, node_size=100, cmap=plt.get_cmap('rainbow'), node_color='r',
                           label="GO term")
    nx.draw_networkx_nodes(G, pos, nodelist=other, node_size=100, cmap=plt.get_cmap('rainbow'), node_color='g',
                           label="Uniprot/ChEBI")

    nx.draw_networkx_labels(G, pos_labels, font_size=10)
    nx.draw_networkx_edges(G, pos, edgelist=has_input_edges, edge_color='g', arrows=True)
    nx.draw_networkx_edges(G, pos, edgelist=has_go_edges, edge_color='r', arrows=True)
    plt.legend(scatterpoints=1)
    plt.show()


def save_graph(edges, nodes):
    file = open("../data/reactome/dd_reactome_nodes.csv", 'w', encoding='utf-8', newline='')
    writer = csv.writer(file)
    writer.writerow(["id", "type", "displayName", "url"])
    for node in nodes.values():
        writer.writerow([node["id"], node["type"], node["displayName"], node.get("url", "")])

    file = open("../data/reactome/dd_reactome_edges.csv", 'w', encoding='utf-8', newline='')
    writer = csv.writer(file)
    writer.writerow(["source", "target"])
    for edge in edges:
        writer.writerow(edge)


# Create a sample graph for selected entries from Reactome
def reactome_to_data_distillery():
    # Get a given number of reactions
    # skip contains a number of lines to skip from the input file (for testing)
    def get_reactome_sample_graph():
        rows = []
        file_csv = "../data/reactome/dd_reactome-links.csv"

        # Set to a positive int to include certain number of reactions into a sample graph
        total = 4
        # Uncomment to start a graph from a given row
        # skip = 23
        with open(file_csv, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            count = 0
            last_id = ""
            for i, row in enumerate(reader):
                # if i < skip:
                #     continue
                if row[0] != last_id:
                    count += 1
                    last_id = row[0]
                if 0 < total < count:
                    break
                rows.append(row)

        nodes = {}
        edges = []
        for row in rows:
            if row[0] not in nodes:
                nodes[row[0]] = {
                    "id": row[0],
                    "type": "Reaction",
                    "displayName": row[1]
                }
            if row[2] not in nodes:
                nodes[row[2]] = {
                    "id": row[2],
                    "type": "Uniprot/ChEBI",
                    "displayName": row[4],
                    "url": row[3]
                }
            if row[5] not in nodes:
                nodes[row[5]] = {
                    "id": row[5],
                    "type": "GO",
                    "displayName": row[7],
                    "url": row[6]
                }
            edges.append((row[0], row[2]))
            edges.append((row[2], row[5]))

        file_csv = "../data/reactome/dd_reactions-go.csv"
        with open(file_csv, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                # When we build a full graph (total = -1), we add all edges reaction -> go term
                # When we build a sample graph (total > 0), we only add edges for selected reactions
                if total < 0:
                    if row[0] not in nodes:
                        nodes[row[0]] = {
                            "id": row[0],
                            "type": "Reaction",
                            "displayName": row[1]
                        }
                if row[0] in nodes:
                    if row[3] not in nodes:
                        nodes[row[3]] = {
                            "id": row[3],
                            "type": "GO",
                            "displayName": row[4],
                            "url": row[5],
                        }
                    edges.append((row[0], row[3]))

        if total > 0:
            plot_reactome_graph(edges, nodes)
        else:
            save_graph(edges, nodes)

    get_reactome_sample_graph()


# reactome_to_data_distillery()


def convert_to_data_distillery_format():
    nodes = {}
    edges = []
    file_nodes = "../data/reactome/dd_reactome_nodes.csv"
    file_edges = "../data/reactome/dd_reactome_edges.csv"

    with open(file_nodes, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None)  # skip the headers
        for row in reader:
            # id,type,displayName,url
            old_id = row[0]
            new_id = "REACTOME " + old_id
            url = row[3]
            if url:
                if url.startswith("http://purl.uniprot.org/uniprot/"):
                    new_id = url.replace("http://purl.uniprot.org/uniprot/", "UNIPROTKB ")
                elif url.startswith("http://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:"):
                    new_id = url.replace("http://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:", "CHEBI ")
                elif url.startswith("https://www.ebi.ac.uk/QuickGO/term/GO:"):
                    new_id = url.replace("https://www.ebi.ac.uk/QuickGO/term/GO:", "GO ")
                else:
                    print(url)
            nodes[old_id] = {
                "node_id": new_id,
                "node_label": row[2],
                "type": row[1]
            }

    with open(file_edges, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None)  # skip the headers
        for row in reader:
            predicate = "has GO term" if nodes[row[1]]["type"] =="GO"  else "has input"
            edges.append([nodes[row[0]]["node_id"], predicate, nodes[row[1]]["node_id"]])

    file = open("../data/reactome/dd_final_nodes.csv", 'w', encoding='utf-8', newline='')
    writer = csv.writer(file)
    writer.writerow(["node_id", "node_label"])
    for node in nodes.values():
        writer.writerow([node["node_id"], node["node_label"]])

    file = open("../data/reactome/dd_final_edges.csv", 'w', encoding='utf-8', newline='')
    writer = csv.writer(file)
    writer.writerow(["subject", "predicate", "object"])
    for edge in edges:
        writer.writerow(edge)


# convert_to_data_distillery_format()

def show_final_graph_examples():
    nodes = {}
    edges = []
    file_edges = "../data/reactome/dd_final_edges.csv"

    with open(file_edges, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None)  # skip the headers
        for i, row in enumerate(reader):
            if row[0] not in nodes:
                nodes[row[0]] = {
                    "node_id": row[0],
                    "type": "GO" if row[0].startswith("GO") else "Reaction" if row[0].startswith("REACTOME") else "Uniprot/ChEBI"
                }
            if row[2] not in nodes:
                nodes[row[2]] = {
                    "node_id": row[2],
                    "type": "GO" if row[2].startswith("GO") else "Reaction" if row[2].startswith("REACTOME") else "Uniprot/ChEBI"
                }
            edges.append((row[0], row[2]))
            if i > 15:
                break

    plot_reactome_graph(edges, nodes)


show_final_graph_examples()