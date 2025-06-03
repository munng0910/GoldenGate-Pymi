#!/bin/bash

# This script use insert and select to get miss records from database source and insert into database target via dblink
# The table need a primary or unique key to make sure that records are unique, this condition very important for WHERE clause. Please note this.
# After completed insert into target database, commit the data.

read -p "TABLE NAME in target database?: " TARGET_TABLE
read -p "Table Name in source database?: " SOURCE_TABLE
read -p "Primary Key?: " PK
read -p "dblink name?: " DBLINK

echo "==> Target Table: $TARGET_TABLE"
echo "==> Source Table: $SOURCE_TABLE"
echo "==> Primary Key: $PK"
echo "==> DB Link: $DBLINK"

# Get column list
COLS_LIST=$(echo "desc ${TARGET_TABLE};" | sqlplus / as sysdba | tail -n +14 | head -n -3 | awk -F' ' {'print $1'} | paste -sd ',' | sed 's| |,|g')

# print insert statement
echo "INSERT INTO ${TARGET_TABLE} (${COLS_LIST}
)
SELECT /*+ parallel(32) index_ffs(c) */
  ${COLS_LIST}
FROM ${SOURCE_TABLE}@${DBLINK} c
WHERE NOT EXISTS (
  SELECT 1 FROM ${TARGET_TABLE} d WHERE d.$PK= c.$PK
);
"

# Prepare and run the insert statement in a single SQL*Plus session
echo "INSERT INTO ${TARGET_TABLE} (${COLS_LIST}
)
SELECT /*+ parallel(32) index_ffs(c) */
  ${COLS_LIST}
FROM ${SOURCE_TABLE}@${DBLINK} c
WHERE NOT EXISTS (
  SELECT 1 FROM ${TARGET_TABLE} d WHERE d.$PK= c.$PK
);
" \
| sqlplus / as sysdba

# commit insert
echo "COMMIT;" | sqlplus / as sysdba