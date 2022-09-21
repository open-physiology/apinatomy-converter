# This is some simplified version of original java code that fills up the apinatomy db
import mysql.connector


# Extra: some code to populate DB (MySQL apinatomy from MyQSL fma)
# Connect to fma
# mysql_db0 = MySQLConnectorExtra("db1o4ros9i7afs", mysql_user, mysql_pwd, "es35.siteground.eu", "3306")
# vessels, mcs = mysql_db0.fma_to_mcs()
# mysql_db = MySQLConnectorExtra("db8h1l9gpgullg", mysql_user, mysql_pwd, "es35.siteground.eu", "3306")
# mysql_db.insert_to_db("structures", vessels)
# mysql_db.insert_to_db("microcirculations", mcs)


class MySQLConnectorExtra:

    def __init__(self, database: str, user: str, password: str, host: str, port: str):
        self.driver = mysql.connector.connect(user=user, password=password, database=database, host=host, port=port)

    # Extra

    def fma_to_mcs(self, type="VEN"):

        vessels: [[str]] = []
        mcs: [[str]] = []
        arterial_circuits = {int}
        venous_circuits = {int}
        # FIXME Dictionary should be pre-populated with vascular circuits
        resource_to_id = dict()
        id_to_resource = dict()

        cursor = self.driver.cursor()
        slot = "arterial supply" if type == "ART" else "venous drainage"
        query = ('select frame, short_value from fma where slot = "{slot}" '
                 'and short_value != "Class" and short_value != "Portion of tissue"').format(slot=slot)
        cursor.execute(query)
        for (frame, short_value) in cursor:
            vessel_trunk = "Trunk of " + short_value.lower()
            # FIXME should be fmaIDs
            vessel_id = resource_to_id.get(vessel_trunk) if vessel_trunk in resource_to_id else str(len(resource_to_id))
            if vessel_trunk not in resource_to_id:
                resource_to_id[vessel_trunk] = vessel_id
                id_to_resource[vessel_id] = vessel_trunk
                vessels.append([vessel_id, vessel_trunk, "", ""])
                # FIXME add to vascular circuits

            if type == "ART":
                arterial_circuits.add(vessel_id)
            else:
                venous_circuits.add(vessel_id)

            mc = bytes(frame)
            organ_id = resource_to_id[mc] if mc in resource_to_id else str(len(resource_to_id))
            mcs.append([vessel_id, organ_id, type, "Y"])

        cursor.close()
        self.driver.close()
        return vessels, mcs

    def insert_to_db(self, table_name, data):
        parts = []
        for row in data:
            parts.append('("' + '", "'.join(row) + '")')
        data = ', '.join(parts)
        query = 'INSERT IGNORE INTO {table_name} VALUES {data}'.format(table_name=table_name, data=data)
        # Enable if the query data is right
        print(query)
        # cursor = self.driver.cursor()
        # cursor.execute(query)
        # cursor.close()

