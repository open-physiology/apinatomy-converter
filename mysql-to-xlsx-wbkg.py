# Convert vascular data from MySQL to Neo4J

import mysql.connector
import pandas as pd
import gspread
import json


class MySQLApinatomyDB:

    def __init__(self, database: str, user: str, password: str, host: str, port: str):
        self.driver = mysql.connector.connect(user=user, password=password, database=database, host=host, port=port)

    def query_lyphs(self):
        cursor = self.driver.cursor()
        cursor.execute('SELECT * FROM lyphs')
        col_names = cursor.column_names
        items = []
        for item in cursor:
            row = dict(zip(col_names, item))
            row.pop('num_id')
            row['id'] = row.pop('ID')
            # row['id'] = re.sub('\t+', '', row['id'])
            row['ontologyTerms'] = row.pop('ontologyTerm')
            row['isTemplate'] = 'TRUE' if row.pop('isTemplate') == 1 else ''
            row['name'] = row.pop('label')
            items.append(row)
        cursor.close()
        return items

    def query_chains(self):
        cursor = self.driver.cursor()
        cursor.execute('SELECT * FROM chains')
        col_names = cursor.column_names
        items = []
        for item in cursor:
            row = dict(zip(col_names, item))
            row.pop('num_id')
            row['id'] = row.pop('ID')
            row['name'] = row.pop('label')
            row['lyphs'] = row.pop('lyph_sequence')
            row['wiredTo'] = row.pop('wire_id')
            items.append(row)
        cursor.close()
        return items

    def query_materials(self):
        cursor = self.driver.cursor()
        cursor.execute('SELECT * FROM materials')
        col_names = cursor.column_names
        items = []
        for item in cursor:
            row = dict(zip(col_names, item))
            row.pop('num_id')
            row.pop('provenance')
            items.append(row)
        cursor.close()
        return items

    def insert_lyphs(self, rows_to_insert):
        cursor = self.driver.cursor()
        for row in rows_to_insert:
            row["isTemplate"] = 1 if row['isTemplate'] == 'TRUE' else 0
        rows = [tuple(d.values()) for d in rows_to_insert]
        # WS: id,ontologyTerms,name,isTemplate,topology,layers,supertype,internalLyphs,internalLyphsInLayers,hostedBy
        sql = "INSERT INTO lyphs (ID, ontologyTerm, label, isTemplate, topology, layers, supertype, hostedBy, " \
              "internalLyphs, internalLyphsInLayers) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.executemany(sql, rows)
        self.driver.commit()
        cursor.close()

    def insert_materials(self, rows_to_insert):
        cursor = self.driver.cursor()
        rows = [tuple(d.values()) for d in rows_to_insert]
        sql = "INSERT INTO materials (id, ontologyTerms, name, materials) VALUES (%s, %s, %s, %s)"
        cursor.executemany(sql, rows)
        self.driver.commit()
        cursor.close()

    def insert_chains(self, rows_to_insert):
        cursor = self.driver.cursor()
        rows = [tuple(d.values()) for d in rows_to_insert]
        sql = "INSERT INTO chains (id, name, lyphs, wiredTo) VALUES (%s, %s, %s, %s)"
        cursor.executemany(sql, rows)
        self.driver.commit()
        cursor.close()


def create_local_excel(df_lyphs, df_chains, df_materials, file_path):
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
    # main
    df_main = pd.DataFrame(columns=["id", "name", "author", "namespace", "description", "imports"])
    df_main.loc[len(df_main.index)] = ["wbrcm", "TOO-map-linked reference connectivity model", "Bernard de Bono",
                                       "wbkg", "Imported from mySQL",
                                       "https://raw.githubusercontent.com/open-physiology/apinatomy-models/master/models/too-map/source/too-map.json"]
    df_main.to_excel(writer, sheet_name='main', index=False)
    # lyphs, materials, chains
    df_lyphs.to_excel(writer, sheet_name='lyphs', index=False)
    df_chains.to_excel(writer, sheet_name='chains', index=False)
    df_materials.to_excel(writer, sheet_name='materials', index=False)
    # local conventions
    local_conventions = {
        "prefix": ["UBERON", "CHEBI", "FMA", "GO", "ILX", "NLX", "SAO", "PMID", "EMAPA", "CL", "NCBITaxon", "wbkg",
                   "too"],
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
    writer.save()


FILE_NAME = './data/wbkg2.xlsx'

# login = json.load(open('./data/wbkg_db.json', 'r'))
login = json.load(open('./data/wbkg_db_nk.json', 'r'))

db_name, db_user, db_pwd, db_server, db_port = (login.values())
mysql_db = MySQLApinatomyDB(db_name, db_user, db_pwd, db_server, db_port)

gc = gspread.service_account(filename='./data/service_account.json')
# sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1BcUBExy-kk-03ceeFuuz8comX3-O0xL1_owTkmZQTCU/edit#gid=1006273879")
sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1h1uO8BZ6BWt55YPZqs0TOJZ8LeughISsnlcDgEw1f_U/edit#gid=1006273879")


# Replaces the spreadsheet completely (not a good option as we lose editing history
def replace_sheet(df, name):
    columns = df.columns.tolist()
    lst = df.values.tolist()
    ws = sh.worksheet(name)
    sh.del_worksheet(ws)
    ws = sh.add_worksheet(title=name, rows=len(lst) + 1, cols=len(columns))
    ws.insert_rows(lst)
    ws.insert_rows([columns])
    ws.format(["A1:Z1"], {"textFormat": {"bold": True}})


# Match two given rows from worksheet and DB table
def compare_rows(db_row, ws_row):
    diff = []
    for col_name in db_row:
        val_db = db_row[col_name].replace(" ", "") if isinstance(db_row[col_name], str) else db_row[col_name]
        val_ws = ws_row[col_name].replace(" ", "") if isinstance(ws_row[col_name], str) else ws_row[col_name]
        if val_db is None:
            val_db = ""
        if str(val_db) != str(val_ws):
            diff.append(col_name)
    return diff


# print found differences
def print_differences(name, missing, changed, extra):
    print("The DB " + name + " not found in the WS:")
    for entry in missing:
        print(entry)
    print()
    print("The DB " + name + " that differ in WS:")
    for entry in changed:
        print(entry)
    print()
    print("The WS " + name + " not found in the DB:")
    for entry in extra:
        print(entry)
    print()

# DB -> WS


# Insert rows with new DB identifiers to the WS
def insert_to_ws(dataset, db_records, headers, name):
    print("Inserting to WS:", len(dataset))
    rows_to_insert = filter(lambda row: row["id"] in dataset, db_records)
    new_rows = []
    for x in rows_to_insert:
        row = []
        for h in headers:
            row.append(x[h] if h in x else '')
        new_rows.append(row)
    for row in new_rows:
        print(row)
    sh.values_append(name, {'valueInputOption': 'USER_ENTERED'}, {'values': new_rows})


# Delete rows with no DB identifiers from the WS
def delete_from_ws(dataset, ws):
    print("Deleting from WS:", len(dataset))
    offset = 0
    for el in dataset:
        ws.delete_rows(el[1]-offset)
        offset += 1


# Update properties of records in WS if they are different
def update_in_ws(dataset, ws):
    print("Updating in WS:", len(dataset))
    headers = ws.row_values(1)
    for changed_row in dataset:
        for d in changed_row:
            dataset = d["ID"].strip()
            cell = ws.find(dataset, in_column=headers.index("id") + 1)
            if cell:
                col = headers.index(d["COL"]) + 1
                if col > 0:
                    print("Updating ({},{}):".format(cell.row, col), d["ID"], ": {} -> {}".format(d["WS"], d["DB"]) )
                    ws.update_cell(cell.row, col, d["DB"])
            else:
                print("Row with given ID not found - skipping...")

# WS -> DB


# Insert
def insert_to_db(dataset, ws, name):
    print("Inserting to DB: ", len(dataset))
    ws_records = ws.get_all_records()
    if len(dataset) == 0:
        return
    ids = list(zip(*dataset))[0]
    rows_to_insert = [row for row in ws_records if row['id'] in ids]
    if name == 'lyphs':
        mysql_db.insert_lyphs(rows_to_insert)
    if name == 'chains':
        mysql_db.insert_chains(rows_to_insert)
    if name == 'materials':
        mysql_db.insert_materials(rows_to_insert)


def delete_from_db(dataset, ws, name):
    print("Deleting from DB: ", len(dataset))


def update_in_db(dataset, ws, name):
    print("Updating in DB: ", len(dataset))


# Compare worksheet with DB table
def compare_sheet(df, name):
    db_records = df.to_dict('records')
    ws = sh.worksheet(name)
    ws_records = ws.get_all_records()
    print("Comparing", name)
    # in DB, no WS
    missing = []
    # in WS, no DB
    extra = []
    # WS != DB
    changed = []
    for i, db_row in enumerate(db_records):
        j = 0
        while j < len(ws_records):
            if ws_records[j]['id'].strip() == db_row["id"].strip():
                diff = compare_rows(db_row, ws_records[j])
                if len(diff) > 0:
                    changed.append([{
                        "ID": db_row["id"].strip(),
                        "COL": col_name,
                        "DB": db_row[col_name],
                        "WS": ws_records[j][col_name]} for col_name in diff])
                break
            j += 1
        if j >= len(ws_records):
            missing.append(db_row["id"])

    for i, ws_row in enumerate(ws_records):
        j = 0
        while j < len(db_records):
            if db_records[j]['id'].strip() == ws_row["id"].strip():
                break
            j += 1
        if j >= len(db_records):
            extra.append([ws_row["id"], i+2])

    print_differences(name, missing, changed, extra)

    # DB -> WS
    choice = input("DB -> WS (y)?")
    if choice == "y":
        choice = input("Insert " + name + " to WS (y)?")
        if choice == "y":
            headers = ws.row_values(1)
            insert_to_ws(missing, db_records, headers, name)

        choice = input("Delete " + name + " from WS (y)?")
        if choice == "y":
            delete_from_ws(extra, ws)

        choice = input("Update " + name + " in WS (y)?")
        if choice == "y":
            update_in_ws(changed, ws)
    else:
        choice = input("WS -> DB (y)?")
        if choice == "y":
            # WS -> DB

            choice = input("Insert " + name + " to DB (y)?")
            if choice == "y":
                insert_to_db(extra, ws, name)

            choice = input("Delete " + name + " from DB (y)?")
            if choice == "y":
                delete_from_db(missing, ws, name)

            choice = input("Update " + name + " in DB (y)?")
            if choice == "y":
                update_in_db(changed, ws, name)


# Commented lines are for reading from local excel file instead of MySQL

yes_to_all = False

# Lyphs
action = 'y' if yes_to_all else input("Compare lyphs (y)?")
if action == 'y':
    df_lyphs = pd.DataFrame.from_records(mysql_db.query_lyphs())
    # df_lyphs = pd.read_excel(FILE_NAME, sheet_name='lyphs',dtype=str)
    # df_lyphs = df_lyphs.replace(np.nan, '')
    df_lyphs = df_lyphs[['id', 'ontologyTerms', 'name',	'isTemplate', 'topology', 'layers',	'supertype', 'internalLyphs',
                         'internalLyphsInLayers', 'hostedBy']]
    lyphs = df_lyphs.values.tolist()
    compare_sheet(df_lyphs, "lyphs")

# Chains
action = 'y' if yes_to_all else input("Compare chains (y)?")
if action == 'y':
    df_chains = pd.DataFrame.from_records(mysql_db.query_chains())
    # df_chains = pd.read_excel(FILE_NAME, sheet_name='chains',dtype=str)
    # df_chains = df_chains.replace(np.nan, '')
    chains = df_chains.values.tolist()
    compare_sheet(df_chains, "chains")

# Materials
action = 'y' if yes_to_all else input("Compare materials (y)?")
if action == 'y':
    df_materials = pd.DataFrame.from_records(mysql_db.query_materials())
    # df_materials = pd.read_excel(FILE_NAME, sheet_name='materials',dtype=str)
    # df_materials = df_materials.replace(np.nan, '')
    materials = df_materials.values.tolist()
    compare_sheet(df_materials, "materials")

# Save imported data in a local Excel file
# create_local_excel(df_lyphs, df_chains, df_materials, FILE_NAME)
