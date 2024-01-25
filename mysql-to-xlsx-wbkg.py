# Synchronize MySQL WBKG with Google spreadsheet WBKG

import mysql.connector
import pandas as pd
import gspread
import json
import gspread_dataframe

# Map ApiNATOMY schema lyph fields to MySQL WBKG DB
def get_db_lyphs_name(name):
    col_map = {
        'ontologyTerms': 'ontologyTerm',
        'name'   : 'label'
    }
    return col_map[name] if name in col_map else name


# Map ApiNATOMY schema chain fields to MySQL WBKG DB
def get_db_chains_name(name):
    col_map = {
        'lyphs': 'lyph_sequence',
        'wiredTo': 'wire_id',
        'name'   : 'label'
    }
    return col_map[name] if name in col_map else name


# Class to handle CRUD operations on MySQL WBKG DB

    def __init__(self, database: str, user: str, password: str, host: str, port: str):
        self.driver = mysql.connector.connect(user=user, password=password, database=database, host=host, port=port)

    # Read DB lyphs and map to ApiNATOMY schema
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
            row['name'] = row.pop('label')
            row['isTemplate'] = 'TRUE' if row.pop('isTemplate') == 1 else ''
            items.append(row)
        cursor.close()
        return items

    # Read DB chains and map to ApiNATOMY schema
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

    # Read DB materials and map to ApiNATOMY schema
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

    # Read DB coalescences (TODO: create table)
    def query_coalescences(self):
        cursor = self.driver.cursor()
        cursor.execute('SELECT * FROM coalescences')
        col_names = cursor.column_names
        items = []
        for item in cursor:
            row = dict(zip(col_names, item))
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

    def insert_coalescences(self, rows_to_insert):
        cursor = self.driver.cursor()
        for row in rows_to_insert:
            if "color" in row:
                del row["color"]
        rows = [tuple(d.values()) for d in rows_to_insert]
        sql = "INSERT INTO coalescences ({0}) VALUES ({1})".format(COALESCENCE_COLUMNS_STR, COALESCENCE_VARS_STR)
        cursor.executemany(sql, rows)
        self.driver.commit()
        cursor.close()

    def delete_list(self, dataset, name):
        if len(dataset) == 0:
            return
        cursor = self.driver.cursor()

        # sql = "DELETE FROM %s WHERE id in (%s)"
        # cursor.execute(sql, (name, ",".join(f"'{w}'" for w in dataset)))
        sql = "DELETE FROM {0} WHERE id in ({1})".format(name, ",".join(f"'{w}'" for w in dataset))
        cursor.execute(sql)
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
                col = get_db_lyphs_name(row['COL'])
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
                col = get_db_chains_name(row['COL'])
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


# login = json.load(open('./data/wbkg_db.json', 'r'))
login = json.load(open('./data/wbkg_db_nk.json', 'r'))

db_name, db_user, db_pwd, db_server, db_port, ws_url, db_backup = (login.values())
mysql_db = MySQLApinatomyDB(db_name, db_user, db_pwd, db_server, db_port)

gc = gspread.service_account(filename='./data/service_account.json')

# For testing material and lyph editors, reset worksheet URL
ws_url = "https://docs.google.com/spreadsheets/d/1PmuQTQZ2xf1EJRBREO6ACd59nkNtVn_6glV4AaROvGk/edit#gid=1423037257"


def create_local():
    print("Creating Google spreadsheet from local file '.data/wbrcm-converted'...")
    sh = gc.create('wbkg')
    local_lyphs = pd.read_excel('./data/wbrcm-converted.xlsx', sheet_name='lyphs', dtype=str)
    local_chains = pd.read_excel('./data/wbrcm-converted.xlsx', sheet_name='chains', dtype=str)
    local_materials = pd.read_excel('./data/wbrcm-converted.xlsx', sheet_name='materials', dtype=str)

    def add_sheet(df, title):
        sh.add_worksheet(title=title, rows=df.shape[0], cols=df.shape[1])
        ws = sh.worksheet(title)
        gspread_dataframe.set_with_dataframe(ws, df)

    add_sheet(local_lyphs, "lyphs")
    add_sheet(local_chains, "chains")
    add_sheet(local_materials, "materials")
    return sh


# Prepare gspread from local file wbcrm-converted.xlsx?
local = input("Update from local file './data/wbrcm-converted.xlsx' (y)?")
sh = create_local() if local == 'y' else gc.open_by_url(ws_url)


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
def compare_sheet(df, ws):
    name = ws.title
    print("Comparing ", name)
    db_records = df.to_dict('records')
    ws_records = ws.get_all_records()
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


def create_db_backup():
    cursor = mysql_db.driver.cursor()
    if db_backup is None:
        print("Backup DB name is not specified!")
        return

    # Method 1
    # Fetch all table names
    table_names = []
    cursor.execute('SHOW TABLES;')
    for record in cursor.fetchall():
        table_names.append(record[0])
    cursor.execute(f"USE {db_backup}")
    for table_name in table_names:
        cursor.execute( f'DROP TABLE IF EXISTS {table_name}')
        cursor.execute(
            f'CREATE TABLE {table_name} SELECT * FROM {db_name}.{table_name}')

    # Method 2
    # backup_dir = 'C:/backup'
    # backup_file = 'mydatabase_backup_' + str(datetime.now().strftime('%Y%m%d_%H%M%S')) + '.bak'
    # command = f"BACKUP DATABASE mydatabase TO DISK='{backup_file}'"
    # cursor.execute(command)

    # Method 3
    # import subprocess
    # now = os.strftime("%Y-%m-%d-%H-%M-%S")
    # subprocess.run(
    #     [
    #         "mysqldump",
    #         "-u",
    #         db_user,
    #         "-p" + db_pwd,
    #         db_name,
    #         ">",
    #         backup_dir + "/" + db_name + "-" + now + ".sql",
    #     ]
    # )
    # subprocess.run(["gzip", backup_dir + "/" + db_name + "-" + now + ".sql"])


action = input("Back-up MySQL (y)?")
if action == 'y':
    create_db_backup()


yes_to_all = False

# List of fields that are used to synchronize data
LYPH_COLUMNS = ['id', 'ontologyTerms', 'name', 'varianceSpecs', 'isTemplate', 'topology', 'layers', 'supertype', 'internalLyphs', 'internalLyphsInLayers', 'hostedBy']
CHAIN_COLUMNS = ['id', 'name', 'lyphs', 'wiredTo']
MATERIAL_COLUMNS = ['id', 'ontologyTerms', 'name', 'materials']
COALESCENCE_COLUMNS = ['id', 'name', 'ontologyTerms', 'lyphs']

LYPH_COLUMNS_STR = ', '.join(map(get_db_lyphs_name,LYPH_COLUMNS))
CHAIN_COLUMNS_STR = ', '.join(map(get_db_chains_name,CHAIN_COLUMNS))
# NK In 'materials all column names coincide
MATERIAL_COLUMNS_STR = ', '.join(MATERIAL_COLUMNS)
COALESCENCE_COLUMNS_STR = ', '.join(COALESCENCE_COLUMNS)

# print("Note, the data is compared based on the following fields:)
# print("Lyphs: ", LYPH_COLUMNS_STR)
# print("Chains:", CHAIN_COLUMNS_STR)
# print("Materials: ", MATERIAL_COLUMNS_STR)

LYPH_VARS_STR = ', '.join(['%s' for x in range(len(LYPH_COLUMNS))])
CHAIN_VARS_STR = ', '.join(['%s' for x in range(len(CHAIN_COLUMNS))])
MATERIAL_VARS_STR = ', '.join(['%s' for x in range(len(MATERIAL_COLUMNS))])

# Lyphs
action = 'y' if yes_to_all else input("Compare lyphs (y)?")
if action == 'y':
    df_lyphs = pd.DataFrame.from_records(mysql_db.query_lyphs())
    df_lyphs = df_lyphs[LYPH_COLUMNS]
    lyphs = df_lyphs.values.tolist()
    ws = sh.worksheet("lyphs")
    compare_sheet(df_lyphs, ws)

# Chains
action = 'y' if yes_to_all else input("Compare chains (y)?")
if action == 'y':
    df_chains = pd.DataFrame.from_records(mysql_db.query_chains())
    df_chains = df_chains[CHAIN_COLUMNS]
    chains = df_chains.values.tolist()
    ws = sh.worksheet("chains")
    compare_sheet(df_chains, ws)

# Materials
action = 'y' if yes_to_all else input("Compare materials (y)?")
if action == 'y':
    df_materials = pd.DataFrame.from_records(mysql_db.query_materials())
    df_materials = df_materials[MATERIAL_COLUMNS]
    materials = df_materials.values.tolist()
    ws = sh.worksheet("materials")
    compare_sheet(df_materials, ws)

