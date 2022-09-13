import ftplib

# import pandas as pd

###########################################################
# Desired places
# # Maitland airport
maitland_airport = 'maitland_airport'
# # Newcastle Nobbys Signal station
newcastle_nobbys_signal_station = 'newcastle_nobbys_signal_station'
# # Paterson (local)
paterson = 'paterson_(tocal)'
# # Scone airport
scone_airport = 'scone_airport'
# # Williamtown RAAF
williamtown_raaf = 'williamtown_raaf'


def getting_ftp_files(place):
    server = 'ftp.bom.gov.au'
    ftp = ftplib.FTP(server)
    directory = f'anon/gen/clim_data/IDCKWCDEA0/tables/nsw/{place}'
    ftp.login()
    ftp.cwd(directory)
    files = ftp.nlst()
    return files


print(getting_ftp_files(maitland_airport))


def gen_create_query(df, warehouse_name, schema_name, tab_name):
    colnames = df.columns
    colnames = [x + ' float' for x in colnames]
    col_info = f"""({', '.join(colnames)})"""
    query = f"""CREATE TABLE IF NOT EXIST {warehouse_name}.{schema_name}.{tab_name} {col_info} ;"""
    return query


def gen_insert_query(df, warehouse_name, schema_name, tab_name):
    list_values = []
    for x in df.__array__():
        test = f'''({",".join(x)})'''
        list_values.append(test)
    query = f"""INSERT INTO {warehouse_name}.{schema_name}.{tab_name} VALUES {','.join(list_values)} ;"""
    return query
