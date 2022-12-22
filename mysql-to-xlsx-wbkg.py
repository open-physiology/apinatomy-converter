# Convert vascular data from MySQL to Neo4J

import mysql.connector
import pandas as pd
import gspread
import json
import numpy as np


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

login = json.load(open('./data/wbkg_db.json', 'r'))
db_name, db_user, db_pwd, db_server, db_port = (login.values())
mysql_db = MySQLApinatomyDB(db_name, db_user, db_pwd, db_server, db_port)

lyphs = mysql_db.query_lyphs()
chains = mysql_db.query_chains()
materials = mysql_db.query_materials()
df_lyphs = pd.DataFrame.from_records(lyphs)
df_chains = pd.DataFrame.from_records(chains)
df_materials = pd.DataFrame.from_records(materials)

# Save imported data in a local Excel file
# create_local_excel(df_lyphs, df_chains, df_materials, FILE_NAME)

# Alternatively - read from local excel file (created by the line above) instead of MySQL
# df_lyphs = pd.read_excel(FILE_NAME, sheet_name='lyphs',dtype=str)
# df_chains = pd.read_excel(FILE_NAME, sheet_name='chains',dtype=str)
# df_materials = pd.read_excel(FILE_NAME, sheet_name='materials',dtype=str)
# df_lyphs = df_lyphs.replace(np.nan, '')
# df_chains = df_chains.replace(np.nan, '')
# df_materials = df_materials.replace(np.nan, '')

# Reorder columns
df_lyphs = df_lyphs[['id', 'ontologyTerms', 'name',	'isTemplate', 'topology', 'layers',	'supertype', 'internalLyphs', 'internalLyphsInLayers', 'hostedBy']]

lyphs = df_lyphs.values.tolist()
chains = df_chains.values.tolist()
materials = df_materials.values.tolist()

gc = gspread.service_account(filename='./data/service_account.json')
# sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1BcUBExy-kk-03ceeFuuz8comX3-O0xL1_owTkmZQTCU/edit#gid=1006273879")
sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1767M7gt18creGSaW1BraTUH9D7oS9WACUowxRRkZJ4w/edit#gid=1006273879")

def replace_sheet(df, name):
    columns = df.columns.tolist()
    lst = df.values.tolist()
    ws = sh.worksheet(name)
    sh.del_worksheet(ws)
    ws = sh.add_worksheet(title=name, rows=len(lst) + 1, cols=len(columns))
    ws.insert_rows(lst)
    ws.insert_rows([columns])
    ws.format(["A1:Z1"], {"textFormat": {"bold": True}})


def db_to_ws(db_row, ws_row):
    pass


def ws_to_db(ws_row, db_row):
    pass


def compare_rows(db_row, ws_row):
    diff = []
    for col_name in db_row:
        val_db = db_row[col_name].replace(" ", "") if isinstance(db_row[col_name], str) else db_row[col_name]
        val_ws = ws_row[col_name].replace(" ", "") if isinstance(ws_row[col_name], str) else ws_row[col_name]
        if val_db != val_ws:
            diff.append(col_name)
    return diff


def compare_sheet(df, name):
    db_records = df.to_dict('records')
    ws = sh.worksheet(name)
    ws_records = ws.get_all_records()
    print("Comparing ", name)
    missing = []
    changed = []
    extra_ws = []
    for i, db_row in enumerate(db_records):
        j = 0
        while j < len(ws_records):
            if ws_records[j]['id'].strip() == db_row["id"].strip():
                diff = compare_rows(db_row, ws_records[j])
                if len(diff) > 0:
                    changed.append([{
                        "ROW": i+2,
                        "COL": col_name,
                        "DB": db_row[col_name],
                        "WS": ws_records[j][col_name]} for col_name in diff])
                break
            j += 1
        if j >= len(ws_records):
            missing.append(db_row["id"])

    print("The DB " + name + " not found in the WS:")
    print(missing)
    print()
    print("The DB " + name + " that differ in WS:")
    for entry in changed:
        print(entry)
    print()

    for i, ws_row in enumerate(ws_records):
        j = 0
        while j < len(db_records):
            if db_records[j]['id'].strip() == ws_row["id"].strip():
                break
            j += 1
        if j >= len(db_records):
            extra_ws.append(ws_row["id"])
    print("The WS " + name + " not found in the DB:")
    print(extra_ws)
    print()


compare_sheet(df_lyphs, "lyphs")
compare_sheet(df_chains, "chains")
compare_sheet(df_materials, "materials")
