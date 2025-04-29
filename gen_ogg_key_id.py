###### Convert from file .xlsx or .csv to .prm file
### import pandas lib as pd
import pandas as pd
import string

### read by default 1st sheet of an excel file
input_file = str(input("Input file (with full path, and include .xlsx or .csv): "))
sheet_name_input = str(input("Sheet name: "))
df_list_table = pd.read_excel(input_file, sheet_name=sheet_name_input)

# Generate parameter
### convert column to string
schema_source_string = '\n'.join(df_list_table['OWNER'].astype(str))
table_source_string = '\n'.join(df_list_table['TABLE_NAME'].astype(str))

### remove delimeter for replicat
list_schema_source = [section.strip().split('\n') for section in schema_source_string.split('~')]
list_table_source = [section.strip().split('\n') for section in table_source_string.split('~')]

### Generate extract prm file
# Add OGG_KEY_ID column

# Loop through the length of any of the lists
for i in range(len(list_schema_source)):
    # Open the file for writing
    # Write file add column OGG_KEY_ID
    with open(f'01.{sheet_name_input}_add_cl_OGG_KEY_ID.sql', 'w', encoding='utf-8') as file:
        # VPBank capture from DR so use different params
        for i in range(len(list_schema_source)):
            for s_src, t_src in zip(list_schema_source[i], list_table_source[i]):
                file.write(
                    f'------------------------------------------------------------- TABLE {s_src}.{t_src}\n'
                    '------------- generate values for OGG_KEY_ID\n'
                    f'alter table {s_src}.{t_src} add OGG_KEY_ID raw(16) invisible;\n\n'
                )

    # Write file modify column OGG_KEY_ID
    with open(f'02.{sheet_name_input}_modify_cl_OGG_KEY_ID.sql', 'w', encoding='utf-8') as file:
        # VPBank capture from DR so use different params
        for i in range(len(list_schema_source)):
            for s_src, t_src in zip(list_schema_source[i], list_table_source[i]):
                file.write(
                    f'------------------------------------------------------------- TABLE {s_src}.{t_src}\n'
                    '------------- generate values for OGG_KEY_ID\n'
                    f'alter table {s_src}.{t_src} modify OGG_KEY_ID default sys_guid();\n\n'
                )

    # Write file to backfill Existing Table Rows with SYS_GUID Values
    with open(f'03.{sheet_name_input}_gen_values_OGG_KEY_ID.sql', 'w', encoding='utf-8') as file:
        # VPBank capture from DR so use different params
        for i in range(len(list_schema_source)):
            for s_src, t_src in zip(list_schema_source[i], list_table_source[i]):
                file.write(
                    f'------------------------------------------------------------- TABLE {s_src}.{t_src}\n'
                    '------------- generate values for OGG_KEY_ID\n'
                    'DECLARE cursor C1 is select ROWID from\n'
                    f'{s_src}.{t_src}\n'
                    'where OGG_KEY_ID is null;\n'
                    'finished number:=0; commit_cnt number:=0; err_msg varchar2(150);\n'
                    'snapshot_too_old exception; pragma exception_init(snapshot_too_old, -1555);\n'
                    'old_size number:=0; current_size number:=0;\n'
                    'BEGIN\n'
                    'while (finished=0) loop\n'
                    'finished:=1;\n'
                    'BEGIN\n'
                    'for C1REC in C1 LOOP\n'
                    f'update {s_src}.{t_src}\n'
                    'set OGG_KEY_ID = sys_guid()\n'
                    'where ROWID = C1REC.ROWID;\n'
                    'commit_cnt:= commit_cnt + 1;\n'
                    'IF (commit_cnt = 10000) then\n'
                    'commit;\n'
                    '            commit_cnt:=0;\n'
                    'END IF;\n'
                    'END LOOP;\n'
                    'EXCEPTION\n'
                    'when snapshot_too_old then\n'
                    '            finished:=0;\n'
                    'when others then\n'
                    '            rollback;\n'
                    '            err_msg:=substr(sqlerrm,1,150);\n'
                    '            raise_application_error(-20555, err_msg);\n'
                    'END;\n'
                    'END LOOP;\n'
                    'IF(commit_cnt > 0) then\n'
                    'commit;\n'
                    'END IF;\n'
                    'END;\n'
                    '/\n'
                    '-------------------------------------------------------------'
                    '-------------------------------------------------------------'
                    '\n\n\n\n\n'
        )