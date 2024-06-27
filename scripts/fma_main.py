import os
import mysql.connector
import csv
import json


def explore_fma():
    login = json.load(open('../credentials/fma_mysql_db.json', 'r'))
    database, user, password, host, port = (login.values())

    mysql_db = mysql.connector.connect(user=user, password=password, database=database, host=host, port=port)

    def create_fma_table(self, columns):
        cursor = mysql_db.driver.cursor()
        cols = ['{} text'.format(x.replace(" ", "_").replace("-", "_")
                                 .replace('partition', 'partition_0').replace('union', 'union_0')) for x in columns]
        col_str = ', '.join(cols)
        query = """CREATE TABLE fma ({})""".format(col_str)
        print(query)
        cursor.execute(query)

    def insert_fma_record(self, col_str, row):
        cursor = self.driver.cursor()
        values = ["'{}'".format(x) if '"' in x else '"{}"'.format(x) for x in row]
        query = "INSERT INTO fma {} VALUES ({})".format(col_str, ','.join(values))
        print(query)
        cursor.execute(query)
        self.driver.commit()

    csv_file_name = "../data/FMA.csv"
    with open(csv_file_name, newline='', encoding='utf-8') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		col_str = ""
		for i, row in enumerate(reader):
			if i == 0:
				cols = ['{}'.format(x.replace(" ", "_").replace("-", "_").replace('partition', 'partition_0').replace('union', 'union_0')) for x in row]
				col_str = "({})".format(', '.join(cols))
				create_fma_table(row)
			if i > 0:
				# print(i)
				# Insert values
				mysql_db.insert_fma_record(col_str, row)
			if i >= 20:
				break


if __name__ == '__main__':
	explore_fma()



