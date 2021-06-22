import psycopg2
import pandas as pd
from datetime import datetime

'''########## connection parameter #############'''
param_dic = {
    "host" : "192.9.201.5",
    "database" : "apollo",
    "user" : "apollo",
    "password" : "apollo"
}

'''########## postgres connect function #############'''
def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

'''########## execute query and return a dataframe #############'''
def postgresql_to_dataframe(conn, select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()

    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    return df

'''########## insert function into history_logs #############'''
def single_insert(conn, insert_req):
    """ Execute a single INSERT request """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()


'''########## logic #############'''

dt_start = datetime.now()
row_count = 0

# Connect to the database
conn = connect(param_dic)

# Column names from source
column_names = ['PKL ID', 'PKL ID1', 'PKL DATUM SAVE', 'PKL Felder', 'PKL Text']

source_select_sql = """select pkl_id, pkl_id1, pkl_datum_save, pkl_felder, pkl_text from history where (pkl_felder like '%12%' or pkl_felder like '%15%' or pkl_felder like '%35%') and pkl_datum_save > '2020-01-01'"""

# Execute the SELECT query
df_source = postgresql_to_dataframe(conn, source_select_sql, column_names)

# Close connection to database
conn.close()
print("Postgres to Dataframe extract")


# Connect to the database
conn = connect(param_dic)

# delete all rows before adding the actual ones
query = """DELETE FROM public.history_logs"""
single_insert(conn, query)

# Close connection to database
conn.close()
print("history_logs deleted")

# Execute the SELECT query
#column_names = ["Felder", "Text"]
#df_final = pd.DataFrame(columns = column_names)

# iterate trough source dataframe and create a list from each column
for index, row in df_source.iterrows():

    # list from each column
    felder_list = row['PKL Felder'].split("")
    text_list = row['PKL Text'].split("")

    # create dictionary from the column lists
    dict_inter = {'Felder': felder_list, 'Text': text_list}

    # create dataframe from dictionary
    df_inter = pd.DataFrame.from_dict(dict_inter)

    # add pkl_id, pkl_id1 and pkl_datum_save to dataframe
    df_inter['PKL Id'] = row['PKL ID']
    df_inter['PKL Id1'] = row['PKL ID1']
    df_inter['PKL Datum Save'] = row['PKL DATUM SAVE']

    df_inter.dropna(inplace=True)

    # append to final dataframe
    #df_final = df_final.append(df_inter, ignore_index=True)

    # iterate trough the final dataframe to add the relevant once to history_logs
    for index, row_final in df_inter.iterrows():

        # get felder row value
        felder = row_final['Felder']

        # transform from string to integer
        if felder != "":
            felder_int = int(felder)

        # extract text
        text = row_final['Text']
        pkl_id = row_final['PKL Id']
        pkl_id1 = row_final['PKL Id1']
        pkl_datum_save = row_final['PKL Datum Save']

        print(type(pkl_datum_save))

        # define the relevant felder values and insert only these
        if felder_int == 12 or felder_int == 15 or felder_int == 35:

            if text != "" and len(text) > 9:

                # split day, month and year
                text_list_single = text.split(".")
                try:
                    day = text_list_single[0]
                    month = text_list_single[1]
                    year = text_list_single[2]
                    day_int = int(day)
                    month_int = int(month)
                    year_int = int(year)
                except (ValueError, IndexError):
                    day = 1
                    month = 1
                    year = 1900

                # generate datetime object
                date = datetime(year_int, month_int, day_int)

            # Connect to the database
            conn = connect(param_dic)

            # insert query, datetime escaped with '%s'
            query = """
            INSERT into history_logs(pkl_id, pkl_id1, pkl_datum_save, feld, datum) values(%s, %s, '%s', %s,'%s');
            """ % (pkl_id, pkl_id1, pkl_datum_save, felder_int, date)
            single_insert(conn, query)
            row_count = row_count + 1
            # Close connection to database
            conn.close()


dt_end = datetime.now()

print(dt_start)
print(dt_end)

# Connect to the database
conn = connect(param_dic)

query = """
INSERT into script_logs(start, "end", row_count) values('%s','%s', %s);
""" % (dt_start, dt_end, row_count)
single_insert(conn, query)

# Close the connection
conn.close()