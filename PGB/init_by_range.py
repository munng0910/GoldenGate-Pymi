#!/usr/bin/python

import pandas as pd
import os
import string

list_trail_1 = [alp+str(num) for alp in list(string.ascii_lowercase) for num in range(0,10) ]
list_trail_2 = [alp+str(num) for alp in list(string.ascii_uppercase) for num in range(0,10) ]
list_trail_3 = [alp1+alp2 for alp1 in list(string.ascii_uppercase) for alp2 in list(string.ascii_lowercase) ]
list_trail_4 = [alp2+alp1 for alp1 in list(string.ascii_uppercase) for alp2 in list(string.ascii_lowercase) ]
list_trail = list_trail_1 + list_trail_2 + list_trail_3 + list_trail_4

"""
def read_input_file(input_file, sheet_name=None):
    try:
        if input_file.endswith('.xlsx'):
            df = pd.read_excel(input_file, sheet_name=sheet_name)
        elif input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        else:
            raise ValueError("Unsupported file type. Please provide a .xlsx or .csv file.")
        return df
    except FileNotFoundError:
        print(f"Error: File {input_file} not found.")
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise
"""

def write_init_extract_prm(prefix_ext_name, full_path_dirdat, alias_source, table_name, num_of_range, condition_column, min_num, max_num):
    step = (max_num - min_num) // (num_of_range-2)
    start_number = min_num  
    for i in range(1, num_of_range+1):
        ext_name = prefix_ext_name + str("{:03d}".format(i))
        end_number = start_number + step
        print(start_number, end_number)
        # Adjust WHERE clause based on end_number
        if end_number < max_num:
            with open(f'{ext_name}.prm', 'w') as file:
                file.write(
                    f'EXTRACT {ext_name}\n'
                    f'EXTFILE {full_path_dirdat}/{list_trail[i-1]}, MEGABYTES 2000, MAXFILES 999\n'
                    f'USERIDALIAS {alias_source}\n'
                    'REPORTCOUNT EVERY 1 SECONDS, RATE\n'
                    f'DISCARDFILE ./dirrpt/{ext_name}.dsc, APPEND\n'
                    f"TABLE {table_name}, WHERE {condition_column} > {start_number} AND {condition_column} < {end_number};\n"
                )
            print(f"Extract parameter {ext_name}.prm has been created. Please COPY this file to $OGG_HOME/dirprm and do not rename the file.")
            start_number = end_number
        elif end_number > max_num:
            with open(f'{ext_name}.prm', 'w') as file:
                file.write(
                    f'EXTRACT {ext_name}\n'
                    f'EXTFILE {full_path_dirdat}/{list_trail[i-1]}, MEGABYTES 2000, MAXFILES 999\n'
                    f'USERIDALIAS {alias_source}\n'
                    'REPORTCOUNT EVERY 1 SECONDS, RATE\n'
                    f'DISCARDFILE ./dirrpt/{ext_name}.dsc, APPEND\n'
                    f"TABLE {table_name}, WHERE {condition_column} > {start_number};\n"
                )
            print(f"Extract parameter {ext_name}.prm has been created. Please COPY this file to $OGG_HOME/dirprm and do not rename the file.")

    print(f"mkdir {full_path_dirdat}")
    print(f"unzip {ext_name}.zip -d $OGG_HOME/dirprm")

def write_replicat_prm(prefix_rep_name, source_table, topic_name):
    for i in range(1, num_trail+1):
        rep_name = prefix_rep_name + str("{:03d}".format(i))
        with open(f'{rep_name}.prm', 'w') as file:
            file.write(
                f'REPLICAT {rep_name}\n'
                f'TARGETDB LIBFILE /data/oggma217/lib/libggjava.so SET property=/data/oggma217_BigData/etc/conf/ogg/confluent_avro_hex.props\n'
                f'discardfile ./dirrpt/{rep_name}.dsc, append, megabytes 4000\n'
                f'GROUPTRANSOPS 200000\n'
                f'REPORTCOUNT EVERY 1 SECONDS, RATE\n\n'
                f'MAP {source_table}, TARGET {topic_name};'
            )

        with open('add_replicat.txt', 'a') as file:
            file.write(
                f'add replicat {rep_name}, exttrail {full_path_dirdat}/{list_trail[i-1]}\n'
            )

    print(f"Replicat parameter {rep_name}.prm has been created. Please COPY this file to $OGG_HOME/dirprm and do not rename the file.")

if __name__ == "__main__":
    program_options = int(input('Do you want to create Extract (0) or Replicat (1)? '))
    full_path_dirdat = input('Full path of dirdat (you do need to create this path before start extract): ').strip().rstrip('/')
    
    if program_options == 0:
        prefix_ext_name = input("Extract name (E_T24 then name will be E_T2401): ").strip().upper()
        alias_source = input('Alias database source: ').strip()
        num_of_range = int(input('How many range you want to use for this extract? '))
        condition_column = input('Condition Column: ').strip().upper()
        table_name = input("Input your table name with format OWENER.TABLE: ").strip().upper()
        max_num = int(input("Max number in the column you want to use: "))
        min_num = int(input("Min number in the column you want to use: "))
        write_init_extract_prm(prefix_ext_name, full_path_dirdat, alias_source, table_name, num_of_range, condition_column, min_num, max_num )
    else:
        prefix_rep_name = input("Replicat name (R_T24 then name will be R_T2401): ").strip().upper()
        source_table = input("Input your table name with format OWENER.TABLE: ").strip().upper()
        topic_name = input("Input your topic name with format OWENER.TABLE: ").strip().upper()
        num_trail = int(input("How many trail you need to replicat?"))
        write_replicat_prm(prefix_rep_name, source_table, topic_name)