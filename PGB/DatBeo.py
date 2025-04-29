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

def write_init_extract_prm(prefix_ext_name, full_path_dirdat, alias_source, table_name, num_of_range, condition_column):
    for i in range(1, int(num_of_range)+1):
        ext_name = prefix_ext_name + str("{:02d}".format(i))
        with open(f'{ext_name}.prm', 'w') as file:
            file.write(
                f'EXTRACT {ext_name}\n'
                f'EXTFILE {full_path_dirdat}/{list_trail[i-1]}, MEGABYTES 2000, MAXFILES 999\n'
                f'USERIDALIAS {alias_source}\n'
                'REPORTCOUNT EVERY 1 SECONDS, RATE\n'
                f'DISCARDFILE ./dirrpt/{ext_name}.dsc, APPEND\n'
                f'TABLE {table_name}, FILTER(@RANGE({i}, {num_of_range}, {condition_column})),  SQLPREDICATE "AS OF SCN {scn_number}";'
            )

            print(f"Extract parameter {ext_name}.prm has been created. Please COPY this file to $OGG_HOME/dirprm and do not rename the file.")
    
    print('#'*40)
    print(f"mkdir {full_path_dirdat}")
    print(f"unzip {ext_name}.zip -d $OGG_HOME/dirprm")

'''
def write_replicat_prm(rep_name, alias_target, list_schema_source, list_table_source, list_schema_target, list_table_target):
    with open(f'{rep_name}.prm', 'w') as file:
        file.write(
            f'REPLICAT {rep_name}\n'
            f'USERIDALIAS {alias_target}\n'
            f'discardfile ./dirrpt/{rep_name}.dsc, append, megabytes 4000\n'
            f'GROUPTRANSOPS 200000\n'
            f'REPORTCOUNT EVERY 1 SECONDS, RATE\n\n'
        )

        for s_src, t_src, s_tgt, t_tgt in zip(list_schema_source, list_table_source, list_schema_target, list_table_target):
            file.write(f'MAP {s_src}.{t_src}, TARGET {s_tgt}.{t_tgt};\n')

    print(f"Replicat parameter {rep_name}.prm has been created. Please COPY this file to $OGG_HOME/dirprm and do not rename the file.")

def generate_prm(df, program_options):
    list_schema_source = df['OWNER'].astype(str).tolist()
    list_table_source = df['TABLE_NAME'].astype(str).tolist()

    if program_options == 0:  # Generate extract
        ext_name = input('Extract name (only 7 characters): ').strip().upper()
        alias_source = input('Alias database source: ').strip()
        alias_downstream = input('Alias database downstream: ').strip()
        prefix_trail = input('Prefix of trailfile, this prefix name must be unique: ').strip()
        
        write_init_extract_prm(ext_name, prefix_trail, alias_source, alias_downstream, list_schema_source, list_table_source)
    else:  # Generate replicat
        schema_target = df['Schema_Target'].astype(str).tolist()
        table_target = df['Table_Target'].astype(str).tolist()
        alias_target = input('Alias database target: ').strip()
        
        rep_name = input('Replicat name: ').strip().upper()
        write_replicat_prm(rep_name, alias_target, list_schema_source, list_table_source, schema_target, table_target)
'''

if __name__ == "__main__":
    prefix_ext_name = input("Extract name (E_T24 then name will be E_T2401): ").strip().upper()
    alias_source = input('Alias database source: ').strip()
    full_path_dirdat = input('Full path of dirdat (you do need to create this path before start extract): ').strip().rstrip('/')
    num_of_range = input('How many range you want to use for this extract? ')
    scn_number = int(input('SCN Number: ').strip())
    condition_column = input('Condition Column: ').strip().upper()
    table_name = input("Input your table name with format OWENER.TABLE: ").strip().upper()

    write_init_extract_prm(prefix_ext_name, full_path_dirdat, alias_source, table_name, num_of_range, condition_column)
    #program_options = int(input('Do you want to create Extract (0) or Replicat (1)? '))
    #generate_prm(df_list_table, program_options)