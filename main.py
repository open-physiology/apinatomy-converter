import json
import csv
import nltk


# String match
def collect_label_data(data):
	entries = []
	for obj in data["graphs"]:
		if "nodes" in obj:
			for node in obj["nodes"]:
				if "lbl" in node:
					entry = [node["id"], node["lbl"]]
					entries.append(entry)
					if "meta" in node:
						if "synonyms" in node["meta"]:
							synonyms = node["meta"]["synonyms"]
							for x in synonyms:
								entry = [node["id"], x["val"]]
								entries.append(entry)
	return entries


def map_no_snomed_by_labels(icd_file):
	data = json.load(open('data/icd10/Input/mondo.json', 'r', encoding='utf-8'))
	entries_mondo = collect_label_data(data)
	print("Mondo: ", len(entries_mondo))

	data = json.load(open('data/icd10/Input/hp.json', 'r', encoding='utf-8'))
	entries_hpo = collect_label_data(data)
	print("HPO: ", len(entries_hpo))

	def match(entries):
		best_row = None
		best_score = 10000
		for row in entries:
			onto_label = row[1]
			quick_score = nltk.edit_distance(label.split(' '), onto_label.split(' '))
			if quick_score < 4:
				score = nltk.edit_distance(label, onto_label)
				if score < best_score:
					best_row = row
					best_score = score
		return [best_row, best_score]

	entries = []
	with open(icd_file, 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			id = row[0]
			label = row[1]

			[best_mondo, best_mondo_score] = match(entries_mondo)
			[best_hpo, best_hpo_score] = match(entries_hpo)

			codes = []; labels = []; scores = []
			if best_mondo:
				codes.append(best_mondo[0])
				labels.append(best_mondo[1])
				scores.append(best_mondo_score)
			if best_hpo:
				codes.append(best_hpo[0])
				labels.append(best_hpo[1])
				scores.append(best_hpo_score)

			entry = [id, label, codes, labels, scores]
			entries.append(entry)

			print(entry)

	file = open("data/icd10/Output/snomed/result_snomed_no_label_match_1.csv", 'w', encoding='utf-8', newline='')
	writer = csv.writer(file)
	writer.writerow(["icd10_id", "icd10_lbl", "mondo/hbo urls", "mondo/hpo labels", "scores"])
	for entry in entries:
		writer.writerow(entry)


def collect_union():
	entries = []
	id_map = {}
	with open("data/icd10/result_label_match.csv", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			score1 = int(row[4])
			score2 = int(row[7])
			if score1 == 0 or score2 == 0:
				id = row[0]
				entry = [id, row[1], "", "", "", ""]
				if score1 == 0:
					entry[2] = row[2]
					entry[3] = row[3]
				if score2 == 0:
					entry[4] = row[5]
					entry[5] = row[6]
				id_map[id] = entry
				entries.append(entry)

	with open("data/icd10/result.csv", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			id = row[0]
			if id not in id_map:
				id_map[id] = row
				entries.append(row)

	file = open("data/icd10/result_all.csv", 'w', encoding='utf-8', newline='')
	writer = csv.writer(file)
	writer.writerow(["icd10_id", "icd10_lbl", "mondo_id", "mondo_lbl", "hb_id", "hb_lbl"])
	for entry in entries:
		writer.writerow(entry)


def collect_icd_data(data, prefix):
	entries = []
	for obj in data["graphs"]:
		if "nodes" in obj:
			for node in obj["nodes"]:
				if "meta" in node:
					if "xrefs" in node["meta"]:
						xrefs = node["meta"]["xrefs"]
						for x in xrefs:
							if x["val"].startswith(prefix):
								entry = [x["val"], node["id"], node["lbl"]]
								entries.append(entry)
	return entries


def collect_mondo_icd():
	data = json.load(open('data/icd10/Input/mondo.json', 'r', encoding='utf-8'))
	entries = collect_icd_data(data, "ICD10")
	file = open("data/icd10/Helpers/mondo.csv", 'w', encoding='utf-8', newline='')
	writer = csv.writer(file)
	writer.writerow(["val", "id", "lbl"])
	for entry in entries:
		writer.writerow(entry)


def collect_hpo_icd():
	data = json.load(open('data/icd10/Input/hp.json', 'r', encoding='utf-8'))
	entries = collect_icd_data(data, "ICD-10")
	file = open("data/icd10/Helpers/hp.csv", 'w', encoding='utf-8', newline='')
	writer = csv.writer(file)
	writer.writerow(["val", "id", "lbl"])
	for entry in entries:
		writer.writerow(entry)


def collect_mondo_snomed():
	data = json.load(open('data/icd10/Input/mondo.json', 'r', encoding='utf-8'))
	entries = collect_icd_data(data, "SCTID")
	file = open("data/icd10/Helpers/mondo_snomed.csv", 'w', encoding='utf-8', newline='')
	writer = csv.writer(file)
	writer.writerow(["val", "id", "lbl"])
	for entry in entries:
		writer.writerow(entry)


def collect_hpo_snomed():
	data = json.load(open('data/icd10/Input/hp.json', 'r', encoding='utf-8'))
	entries = collect_icd_data(data, "SNOMEDCT_US")
	file = open("data/icd10/Helpers/hp_snomed.csv", 'w', encoding='utf-8', newline='')
	writer = csv.writer(file)
	writer.writerow(["val", "id", "lbl"])
	for entry in entries:
		writer.writerow(entry)


def exact_code_mapping():
	code_map = {}
	with open("data/icd10/Helpers/mondo.csv", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			id = row[0].replace('.', '')
			mapped_id = row[1]
			label = row[2]
			if id not in code_map:
				code_map[id] = [id, mapped_id, label, "", ""]
	with open("data/icd10/Helpers/hp.csv", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			id = row[0].replace('.', '')
			mapped_id = row[1]
			label = row[2]
			if id in code_map:
				code_map[id][3] = mapped_id
				code_map[id][4] = label
			else:
				code_map[id] = [id, "", "", mapped_id, label]

	entries = []
	count = 0
	with open("data/icd10/Input/MPRINT_MarketScan_Phenotypes.csv", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			values = row[1].split(' ')
			id = values[1]
			label = ' '.join(values[2:])
			if id in code_map:
				entry = code_map[id]
				entry.insert(1, label)
				count += 1
				entries.append(entry)

	print(count)

	file = open("data/icd10/result.csv", 'w', encoding='utf-8', newline='')
	writer = csv.writer(file)
	writer.writerow(["icd10_id", "icd10_lbl", "mondo_id", "mondo_lbl", "hb_id", "hb_lbl"])
	for entry in entries:
		writer.writerow(entry)


def snomed():
	entries = []
	# snomed_icd_map = {}
	icd_snomed_map = {}
	with open("data/icd10/snomed/Full/Refset/Map/der2_iisssccRefset_ExtendedMapFull_US1000124_20240901.txt", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter='\t')
		# headers = next(reader)
		# print(len(headers))
		# for key in headers:
		# 	print(key)
		for row in reader:
			entries.append([row[5], row[9], row[10]])
		for entry in entries:
			snomed_id = entry[0]
			icd_id = entry[2].replace(".", "")
			# if snomed_id == "67678004":
			# 	print(entry)
			# if snomed_id not in snomed_icd_map:
			# 	snomed_icd_map[snomed_id] = [icd_id]
			# else:
			# 	if icd_id not in snomed_icd_map[snomed_id]:
			# 		snomed_icd_map[snomed_id].append(icd_id)
			if icd_id not in icd_snomed_map:
				icd_snomed_map[icd_id] = [snomed_id]
			else:
				if snomed_id not in icd_snomed_map[icd_id]:
					icd_snomed_map[icd_id].append(snomed_id)

	# KN Save maps
	# full icd_snomed_map
	with open("data/icd10/Output/icd_snomed.csv", 'w', encoding='utf-8', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(["icd10_id", "snomed_ids"])
		for key in icd_snomed_map:
			writer.writerow([key, icd_snomed_map[key]])

	entries = []
	no_snomed = []
	with open("data/icd10/Input/MPRINT_MarketScan_Phenotypes.csv", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			values = row[1].split(' ')
			id = values[1]
			label = ' '.join(values[2:])
			if id in icd_snomed_map:
				entry = icd_snomed_map[id]
				entry.insert(0, id)
				entry.insert(1, label)
				entries.append(entry)
			else:
				no_snomed.append([id, label])

	with open("data/icd10/Output/snomed/result_snomed.csv", 'w', encoding='utf-8', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(["icd10_id", "icd10_lbl", "snomed_ids"])
		for entry in entries:
			writer.writerow(entry)

	with open("data/icd10/Output/snomed/result_no_snomed.csv", 'w', encoding='utf-8', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(["icd10_id", "icd10_lbl"])
		for entry in no_snomed:
			writer.writerow(entry)


def add_labels_to_snomed():
	code_map = {}
	with open("data/icd10/result_label_found.csv", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			id = row[0]
			code_map[id] = row

	with open("data/icd10/Output/snomed/result_snomed_mondo_hpo_no_mapping.csv", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			id = row[0]
			if id in code_map:
				print(code_map[id])


def exact_code_mapping_snomed():
	code_map = {}
	with open("data/icd10/Helpers/mondo_snomed.csv", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			id = row[0].replace('.', '')
			mapped_id = row[1]
			label = row[2]
			if id not in code_map:
				code_map[id] = [id, mapped_id, label, "", ""]
	with open("data/icd10/Helpers/hp_snomed.csv", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			id = row[0].replace('.', '')
			mapped_id = row[1]
			label = row[2]
			if id in code_map:
				code_map[id][3] = mapped_id
				code_map[id][4] = label
			else:
				code_map[id] = [id, "", "", mapped_id, label]

	entries = []
	with open("data/icd10/Output/snomed/result_snomed.csv", 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			label = row[0]
			icd_id = row[1]
			snomed_codes = row[2:]
			codes = []
			labels = []
			for id in snomed_codes:
				if id in code_map:
					entry = code_map[id]
					# MONDO and HBO codes at positions 1 and 3
					if entry[1] != "":
						codes.append(entry[1])
						labels.append(entry[2])
					if entry[3] != "":
						codes.append(entry[3])
						labels.append(entry[4])
				entry = [icd_id, label, codes, labels]
			if len(codes) == 0:
				entries.append(entry)

	file = open("data/icd10/Output/snomed/result_snomed_mondo_hpo_no_mapping.csv", 'w', encoding='utf-8', newline='')
	writer = csv.writer(file)
	writer.writerow(["icd10_id", "icd10_lbl", "mondo/hbo urls", 'mondo/hpo labels'])
	for entry in entries:
		writer.writerow(entry)


def split_no_snomed_by_label(input_file,ext=""):
	matched = []
	unmatched = []
	with open(input_file, 'r') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		next(reader, None)
		for row in reader:
			codes = row[2]
			if len(codes) > 7:
				matched.append(row)
			else:
				unmatched.append(row[:2])
	with open("data/icd10/result_no_snomed_matched{0}.csv".format(ext), 'w', encoding='utf-8', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(["icd10_id", "icd10_lbl", "mondo/hbo urls", 'mondo/hpo labels'])
		for entry in matched:
			writer.writerow(entry)
	with open("data/icd10/result_no_snomed_unmatched{}.csv".format(ext), 'w', encoding='utf-8', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(["icd10_id", "icd10_lbl"])
		for entry in unmatched:
			writer.writerow(entry)


if __name__ == '__main__':
	# collect_mondo_icd()
	# collect_hpo_icd()
	# exact_code_mapping()
	# collect_union()
	# collect_mondo_snomed()
	# collect_hpo_snomed()
	snomed()
	# exact_code_mapping_snomed()
	# add_labels_to_snomed()

	# map_no_snomed_by_labels("data/icd10/result_snomed_mondo_hpo_no_mapping.csv")
	# map_no_snomed_by_labels("data/icd10/result_no_snomed.csv")
	# split_no_snomed_by_label("data/icd10/result_snomed_no_label_match.csv")
	split_no_snomed_by_label("data/icd10/Output/snomed/result_snomed_no_label_match_1.csv", "_1")

# MATCH (phenotype_code:Code {SAB:'HP'})<-[:CODE]-(phenotype_concept:Concept)<-[r:has_phenotype {SAB:'KF'}]-(kfpt_concept:Concept),
# (phenotype_concept:Concept)-[:PREF_TERM]->(phenotype_term:Term) RETURN DISTINCT phenotype_code.CodeID AS HPO_ID,phenotype_term.name AS HPO_Term



