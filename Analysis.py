import pandas as pd
import snowflake.connector
import os

file = "data_comp.xlsx"

df = pd.read_excel(file, sheet_name='Consolidated', header=None)

tests = {}
current_test = None
data = []

for index, row in df.iterrows():

    first_cell = str(row[0]).strip()

    if first_cell.startswith("TEST"):

        if current_test and data:
            tests[current_test] = pd.DataFrame(data)

        current_test = first_cell
        data = []
        continue

    if row.isnull().all():
        continue

    data.append(row.tolist())

if current_test and data:
    tests[current_test] = pd.DataFrame(data)

all_test_data = ""

for test, table in tests.items():
    all_test_data += f"\n{test}\n"
    all_test_data += table.to_string(index=False)


conn = snowflake.connector.connect(
    user="monopoly22",
    password="8638569740picklU",
    account="JZBSADH-CG45326",
    warehouse="COMPUTE_WH",
    database="DBT_POC",
    schema="DBT_SCHEMA"
)

cursor = conn.cursor()

prompt = f"""
Analyze the following data reconciliation tests.

1. Identify SRC vs TGT mismatches
2. Detect ONLY_IN_SRC records
3. Provide root cause
4. Provide summary for each TEST

Data:
{all_test_data}
"""

query = f"""
SELECT SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
$$ {prompt} $$
);
"""

cursor.execute(query)

result = cursor.fetchone()[0]

with open("analysis_report.txt", "w") as f:
    f.write(result)

print(result)

cursor.close()
conn.close()
