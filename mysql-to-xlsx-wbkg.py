# Synchronize MySQL WBKG with Google spreadsheet WBKG

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
            if "color" in row:
                del row["color"]
        rows = [tuple(d.values()) for d in rows_to_insert]
        # WS: id,ontologyTerms,name,varianceSpecs,isTemplate,topology,layers,supertype,internalLyphs,internalLyphsInLayers,hostedBy

        sql = "INSERT INTO lyphs ({0}) VALUES ({1})".format(LYPH_COLUMNS_STR, LYPH_VARS_STR)
        cursor.executemany(sql, rows)
        self.driver.commit()
        cursor.close()

    def insert_materials(self, rows_to_insert):
        cursor = self.driver.cursor()
        for row in rows_to_insert:
            if "color" in row:
                del row["color"]
        rows = [tuple(d.values()) for d in rows_to_insert]
        sql = "INSERT INTO materials ({0}) VALUES ({1})".format(MATERIAL_COLUMNS_STR, MATERIAL_VARS_STR)
        cursor.executemany(sql, rows)
        self.driver.commit()
        cursor.close()

    def insert_chains(self, rows_to_insert):
        cursor = self.driver.cursor()
        for row in rows_to_insert:
            if "color" in row:
                del row["color"]
        rows = [tuple(d.values()) for d in rows_to_insert]
        sql = "INSERT INTO chains ({0}) VALUES ({1})".format(CHAIN_COLUMNS_STR, CHAIN_VARS_STR)
        cursor.executemany(sql, rows)
        self.driver.commit()
        cursor.close()

    def delete_list(self, dataset, name):
        if len(dataset) == 0:
            return
        cursor = self.driver.cursor()
        sql = "DELETE FROM %s WHERE id in (%s)"
        cursor.execute(sql, (name, ",".join(f"'{w}'" for w in dataset)))
        self.driver.commit()
        cursor.close()

    def update_lyphs(self, dataset):
        if len(dataset) == 0:
            return
        cursor = self.driver.cursor()
        for entries in dataset:
            for row in entries:
                # @Example
                # {'ID': 'lyph-L6-spinal-segment', 'COL': 'ontologyTerms', 'DB': 'ILX:0793358', 'WS': 'ILX:0738432'}
                col = 'ontologyTerm' if row['COL'] == 'ontologyTerms' else 'label' if row['COL'] == 'name' else row['COL']
                val = row['WS']
                if col == 'isTemplate':
                    val = 1 if row['WS'] == 'TRUE' else 0
                sql = "UPDATE lyphs SET {0} = %s WHERE ID = %s".format(col)
                cursor.execute(sql, (val, row['ID']))
        self.driver.commit()
        cursor.close()

    def update_chains(self, dataset):
        if len(dataset) == 0:
            return
        cursor = self.driver.cursor()
        for entries in dataset:
            for row in entries:
                col = 'lyph_sequence' if row['COL'] == 'lyphs' else 'label' if row['COL'] == 'name' else \
                    'wire_id' if row['COL'] == 'wiredTo' else row['COL']
                sql = "UPDATE chains SET {0} = %s WHERE ID = %s".format(col)
                cursor.execute(sql, (row['WS'], row['ID']))
        self.driver.commit()
        cursor.close()

    def update_materials(self, dataset):
        if len(dataset) == 0:
            return
        cursor = self.driver.cursor()
        for entries in dataset:
            for row in entries:
                sql = "UPDATE materials SET {0} = %s WHERE ID = %s".format(row['COL'])
                cursor.execute(sql, (row['WS'], row['ID']))
        self.driver.commit()
        cursor.close()

login = json.load(open('./data/wbkg_db.json', 'r'))
# login = json.load(open('./data/wbkg_db_nk.json', 'r'))

db_name, db_user, db_pwd, db_server, db_port = (login.values())
mysql_db = MySQLApinatomyDB(db_name, db_user, db_pwd, db_server, db_port)

gc = gspread.service_account(filename='./data/service_account.json')
sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1BcUBExy-kk-03ceeFuuz8comX3-O0xL1_owTkmZQTCU/edit#gid=1006273879")
# sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1Y1nPCiRTiM1A8xUQn8NwNOlsz7z32hGd6W6U_ZQOrLY/edit#gid=1006273879")
# sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1h1uO8BZ6BWt55YPZqs0TOJZ8LeughISsnlcDgEw1f_U/edit#gid=1006273879")
# sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1jDtLrfm1Kz25oAcjRGEFu3qXqh5iz1_4lAEaQzuHQDc/edit?pli=1#gid=855732402")


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
    print("The DB " + name + " not found in the WS:", len(missing))
    for entry in missing:
        print(entry)
    print()
    print("The DB " + name + " that differ in WS:", len(changed))
    for entry in changed:
        print(entry)
    print()
    print("The WS " + name + " not found in the DB:", len(extra))
    for entry in extra:
        print(entry)
    print()


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


def delete_from_db(dataset, name):
    print("Deleting from DB: ", len(dataset))
    mysql_db.delete_list(dataset, name)


def update_in_db(dataset, name):
    print("Updating in DB: ", len(dataset))
    if name == 'lyphs':
        mysql_db.update_lyphs(dataset)
    if name == 'chains':
        mysql_db.update_chains(dataset)
    if name == 'materials':
        mysql_db.update_materials(dataset)


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

    if len(missing) > 0 or len(changed) > 0 or len(extra) > 0:
        # DB -> WS
        choice = input("DB -> WS (y)?")
        if choice == "y":
            if len(missing) > 0:
                choice = input("Insert " + name + " to WS (y)?")
                if choice == "y":
                    headers = ws.row_values(1)
                    insert_to_ws(missing, db_records, headers, name)

            if len(extra) > 0:
                choice = input("Delete " + name + " from WS (y)?")
                if choice == "y":
                    delete_from_ws(extra, ws)

            if len(changed) > 0:
                choice = input("Update " + name + " in WS (y)?")
                if choice == "y":
                    update_in_ws(changed, ws)
        else:
            choice = input("WS -> DB (y)?")
            if choice == "y":
                # WS -> DB

                if len(extra) > 0:
                    choice = input("Insert " + name + " to DB (y)?")
                    if choice == "y":
                        insert_to_db(extra, ws, name)

                if len(missing) > 0:
                    choice = input("Delete " + name + " from DB (y)?")
                    if choice == "y":
                        delete_from_db(missing, name)
                if len(changed) > 0:
                    choice = input("Update " + name + " in DB (y)?")
                    if choice == "y":
                        update_in_db(changed, name)


yes_to_all = False

LYPH_COLUMNS = ['id', 'ontologyTerms', 'name', 'varianceSpecs', 'isTemplate', 'topology', 'layers',
                         'supertype', 'internalLyphs', 'internalLyphsInLayers', 'hostedBy']
CHAIN_COLUMNS = ['id', 'name', 'lyphs', 'wiredTo']
MATERIAL_COLUMNS = ['id', 'ontologyTerms', 'name', 'materials']

LYPH_COLUMNS_STR = ', '.join(LYPH_COLUMNS)
CHAIN_COLUMNS_STR = ', '.join(CHAIN_COLUMNS)
MATERIAL_COLUMNS_STR = ', '.join(MATERIAL_COLUMNS)

print(LYPH_COLUMNS_STR)
print(CHAIN_COLUMNS_STR)
print(MATERIAL_COLUMNS_STR)

LYPH_VARS_STR = ', '.join(['%s' for x in range(len(LYPH_COLUMNS))])
CHAIN_VARS_STR = ', '.join(['%s' for x in range(len(CHAIN_COLUMNS))])
MATERIAL_VARS_STR = ', '.join(['%s' for x in range(len(MATERIAL_COLUMNS))])

# Lyphs
action = 'y' if yes_to_all else input("Compare lyphs (y)?")
if action == 'y':
    df_lyphs = pd.DataFrame.from_records(mysql_db.query_lyphs())
    df_lyphs = df_lyphs[LYPH_COLUMNS]
    lyphs = df_lyphs.values.tolist()
    compare_sheet(df_lyphs, "lyphs")

# Chains
action = 'y' if yes_to_all else input("Compare chains (y)?")
if action == 'y':
    df_chains = pd.DataFrame.from_records(mysql_db.query_chains())
    df_chains = df_chains[CHAIN_COLUMNS]
    chains = df_chains.values.tolist()
    compare_sheet(df_chains, "chains")

# Materials
action = 'y' if yes_to_all else input("Compare materials (y)?")
if action == 'y':
    df_materials = pd.DataFrame.from_records(mysql_db.query_materials())
    df_materials = df_materials[MATERIAL_COLUMNS]
    materials = df_materials.values.tolist()
    compare_sheet(df_materials, "materials")
