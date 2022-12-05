"""
link dataset = https://www.kaggle.com/datasets/alibuttj/pizza-flavor-recommender-system
"""

import mysql.connector
import numpy as np
import pandas as pd


#variabel
user = 'root'
password = ''
host = '127.0.0.1'
database = 'pizza'

def preprocessing(df):
    df = pd.read_excel(df)
    LISTNAMEOFPIZZA = df['Name of Pizza'].to_list()
    index = LISTNAMEOFPIZZA.index(np.nan)
    df = df.iloc[:index,:]
    return df

#koneksi ke database
def config(user= user, password=password, host=host, database=database):
	config = mysql.connector.connect(
		host=host,
		user=user,
		passwd=password,
		database=database
	)
	return config

class sql():
    def __init__(self, user, password, host, database) :
        self.config = mysql.connector.connect(
                                                host=host,
                                                user=user,
                                                passwd=password,
                                                database=database
                                            )

    def execute(self, query, values):
        config = self.config
        mycursor = config.cursor()
        self.execute = mycursor.execute(query,values)
        return('oke')
    
    def create_table(self, string_datatype, df, table_name, primary_key_names):
        columns='`{}` int(100) NOT NULL'.format(primary_key_names)

        for x in df.columns:
            if x in string_datatype:
                x = x.replace(' ', '_')
                columns = columns + ", `"+ x + "`" + " varchar(100) NOT NULL"
            else:
                x = x.replace(' ', '_')
                columns = columns + ", `"+ x + "`" + " int(100) NOT NULL"
        
        query = "CREATE TABLE IF NOT EXISTS {} ({})".format(table_name, columns)
        values = ()
        self.execute(query, values)
        return('oke')
        
    def make_primary_key(self, table_name, primary_key_names):
        query = "ALTER TABLE {} ADD PRIMARY KEY ({})".format(table_name, primary_key_names)
        values = ()
        self.execute(query, values)
        return('oke')
       
    def make_auto_increment(self, table_name, primary_key_names):
        query = "ALTER TABLE `{}` MODIFY `{}` int(11) NOT NULL AUTO_INCREMENT".format(table_name, primary_key_names)
        values = ()
        self.execute(query, values)
        return('oke')
        
    def insert(self, query, values):
        config = self.config
        mycursor = config.cursor()
        mycursor.execute(query,values)
        config.commit()
        return mycursor.lastrowid

    def insert_df(self, df, table_name):
        #pembuatan kolom
        column = ''
        for x in df.columns:
            x = x.replace(' ', '_')
            column = column + x + ', '
        column = column[:-2]

        abc = df.to_numpy()
        for x in abc:
            data = ''
            for y in x:
                data = data +"'"+ str(y) +"'"+ ', '
            data = data[:-2]

            query = "INSERT INTO {} ({}) VALUES ({})".format(table_name, column, data)
            values = ()
            self.insert(query, values)

df = preprocessing('Pizza Flavors Sheet.xlsx')

string_datatype = ['Name of Pizza']
table_names= "pizza4" 
primary_key_names = "id_pizza"

abc = sql(user, password, host, database)
abc.create_table(string_datatype, df, table_names, primary_key_names)
abc = sql(user, password, host, database)
abc.make_primary_key(table_names, primary_key_names)
abc = sql(user, password, host, database)
abc.make_auto_increment(table_names, primary_key_names)

abc = sql(user, password, host, database)
abc.insert_df(df, table_names)
