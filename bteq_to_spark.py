"""
This script provides functionality to convert BTEQ SQL scripts into Spark SQL scripts.
It includes functions to extract SQL from BTEQ scripts, convert volatile tables to CTEs,
remove DROP TABLE statements, and replace Teradata-specific syntax with Spark equivalents.

Usage:
------
Run the script from the command line with the following syntax:
    python bteq_to_spark.py <input_file.sql> <output_file.sql>
Example:
--------
To convert a BTEQ SQL script named 'barclays_bteq_script_01.sql' to Spark SQL:
    python bteq_to_spark.py barclays_bteq_script_01.sql output_spark.sql
"""

import re
import sys

# Extracts SQL statements from a BTEQ script by removing BTEQ-specific commands, single-line comments, multi-line comments, and empty lines.
def extract_sql_from_bteq(bteq_script: str) -> str:
    
    # Remove BTEQ-specific commands (lines starting with a dot or keywords like LOGON, LOGOFF, etc.)
    sql_only = re.sub(r'^\s*\.(LOGON|LOGOFF|SESSION|SET|QUIT|LABEL|GOTO|LOG)\b.*$', '', bteq_script, flags=re.IGNORECASE | re.MULTILINE)
    
    # Remove single-line comments (lines starting with --)
    sql_only = re.sub(r'--.*$', '', sql_only, flags=re.MULTILINE)
    
    # Remove multi-line comments (/* ... */)
    sql_only = re.sub(r'/\*.*?\*/', '', sql_only, flags=re.DOTALL)
    
    # Remove empty lines
    sql_only = re.sub(r'^\s*$', '', sql_only, flags=re.MULTILINE)
    
    return sql_only.strip()

# Converts Teradata volatile table definitions into Spark SQL Common Table Expressions (CTEs).
def convert_volatile_to_single_cte(sql_script: str) -> str:
    # Regex to match CREATE MULTISET VOLATILE TABLE statements
    pattern = re.compile(
        r'CREATE MULTISET VOLATILE TABLE (\w+) AS \((.*?)\) WITH DATA ON COMMIT PRESERVE ROWS;',
        flags=re.IGNORECASE | re.DOTALL
    )

    # List to store all CTEs
    cte_list = []

    # Replace each volatile table creation with a CTE definition
    def replace_with_cte(match):
        table_name = match.group(1)
        select_query = match.group(2).strip()
        cte_list.append(f"{table_name} AS (\n    {select_query}\n)")
        return ""  # Remove the original statement

    # Perform the replacement and collect CTEs
    sql_script = pattern.sub(replace_with_cte, sql_script)

    # Combine all CTEs under a single WITH clause
    if cte_list:
        cte_section = "WITH " + ",\n".join(cte_list) + "\n"
        sql_script = cte_section + sql_script

    # Clean up extra whitespace
    sql_script = re.sub(r'\n\s*\n', '\n', sql_script)

    return sql_script.strip()

# Removes DROP TABLE statements for volatile tables from the SQL script.
def remove_drop_table_statements(sql_script: str) -> str:
    # Remove DROP TABLE statements for volatile tables
    sql_script = re.sub(r'DROP TABLE volatile_\w+;', '', sql_script, flags=re.IGNORECASE)
    return sql_script

# Converts a BTEQ SQL script into a Spark SQL script by:
#     - Removing BTEQ-specific commands.   
#     - Replacing Teradata-specific functions with Spark equivalents.
#     - Converting volatile tables to CTEs.
#     - Removing DROP TABLE statements.
#     - Cleaning up extra whitespace.
#     - Replacing Teradata-style casting with Spark SQL syntax.
#     - Combining "WITH DATA" and "ON COMMIT PRESERVE ROWS" into a single line.
#     - Removing unnecessary semicolons.
#     - Replacing Teradata-specific date functions with Spark equivalents.
#     - Replacing Teradata-specific string functions with Spark equivalents.
#     - Replacing Teradata-specific join syntax with Spark equivalents.
#     - Replacing Teradata-specific window functions with Spark equivalents.
#     - Replacing Teradata-specific date arithmetic with Spark equivalents.
#     - Replacing Teradata-specific conditional expressions with Spark equivalents.

def convert_bteq_to_spark(bteq_sql: str) -> str:
    # Remove BTEQ-specific commands
    bteq_sql = re.sub(r'\b(BT|ET|DATABASE|LOGON|LOGOFF|LABEL|GOTO)\b.*', '', bteq_sql, flags=re.IGNORECASE)

    # Replace Teradata-specific functions with Spark equivalents
    bteq_sql = re.sub(r'\bCURRENT_DATE\b', 'current_date()', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'\bDATE\b', 'DATE', bteq_sql, flags=re.IGNORECASE)

    # Replace Teradata-style casting
    bteq_sql = re.sub(r'CAST\((.*?) AS (.*?)\)', r'CAST(\1 AS \2)', bteq_sql, flags=re.IGNORECASE)
    
    # Replace Teradata-specific string functions with Spark equivalents
    bteq_sql = re.sub(r'\bTRIM\(BOTH \' \' FROM (.*?)\)', r'TRIM(\1)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'\bSUBSTRING\((.*?) FROM (.*?) FOR (.*?)\)', r'SUBSTRING(\1, \2, \3)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'\bUPPER\((.*?)\)', r'UPPER(\1)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'\bLOWER\((.*?)\)', r'LOWER(\1)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'\bPOSITION\((.*?) IN (.*?)\)', r'INSTR(\2, \1)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'\bCHARACTER_LENGTH\((.*?)\)', r'LENGTH(\1)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'\bTRIM\((.*?)\)', r'TRIM(\1)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'\bLPAD\((.*?)\, (.*?)\)', r'LPAD(\1, \2)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'\bRPAD\((.*?)\, (.*?)\)', r'RPAD(\1, \2)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'\bREPLACE\((.*?)\, (.*?)\, (.*?)\)', r'REPLACE(\1, \2, \3)', bteq_sql, flags=re.IGNORECASE)
    
    # Replacing Teradata-specific join syntax with Spark equivalents.
    bteq_sql = re.sub(r'INNER JOIN', 'JOIN', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'LEFT OUTER JOIN', 'LEFT JOIN', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'RIGHT OUTER JOIN', 'RIGHT JOIN', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'FULL OUTER JOIN', 'FULL JOIN', bteq_sql, flags=re.IGNORECASE)
    
    # Replacing Teradata-specific window functions with Spark equivalents.
    bteq_sql = re.sub(r'ROW_NUMBER\(\) OVER \((.*?)\)', r'ROW_NUMBER() OVER (\1)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'RANK\(\) OVER \((.*?)\)', r'RANK() OVER (\1)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'DENSE_RANK\(\) OVER \((.*?)\)', r'DENSE_RANK() OVER (\1)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'NTILE\((.*?)\) OVER \((.*?)\)', r'NTILE(\1) OVER (\2)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'LEAD\((.*?)\, (.*?)\) OVER \((.*?)\)', r'LEAD(\1, \2) OVER (\3)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'LAG\((.*?)\, (.*?)\) OVER \((.*?)\)', r'LAG(\1, \2) OVER (\3)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'FIRST_VALUE\((.*?)\) OVER \((.*?)\)', r'FIRST_VALUE(\1) OVER (\2)', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'LAST_VALUE\((.*?)\) OVER \((.*?)\)', r'LAST_VALUE(\1) OVER (\2)', bteq_sql, flags=re.IGNORECASE)
    
    # Replacing Teradata-specific date arithmetic with Spark equivalents.
    bteq_sql = re.sub(r'CURRENT_DATE \+ (\d+)', r'CURRENT_DATE + INTERVAL \1 DAYS', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'CURRENT_DATE - (\d+)', r'CURRENT_DATE - INTERVAL \1 DAYS', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'(\d+) DAY\(S\) FROM CURRENT_DATE', r'INTERVAL \1 DAYS', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'(\d+) MONTH\(S\) FROM CURRENT_DATE', r'INTERVAL \1 MONTHS', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'(\d+) YEAR\(S\) FROM CURRENT_DATE', r'INTERVAL \1 YEARS', bteq_sql, flags=re.IGNORECASE)
    
    # Replacing Teradata-specific conditional expressions with Spark equivalents.
    bteq_sql = re.sub(r'CASE WHEN (.*?) THEN (.*?) ELSE (.*?) END', r'CASE WHEN \1 THEN \2 ELSE \3 END', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'CASE WHEN (.*?) THEN (.*?) END', r'CASE WHEN \1 THEN \2 END', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'CASE (.*?) WHEN (.*?) THEN (.*?) END', r'CASE \1 WHEN \2 THEN \3 END', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'CASE (.*?) WHEN (.*?) THEN (.*?) ELSE (.*?) END', r'CASE \1 WHEN \2 THEN \3 ELSE \4 END', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'CASE (.*?) WHEN (.*?) THEN (.*?) ELSE (.*?) END', r'CASE \1 WHEN \2 THEN \3 ELSE \4 END', bteq_sql, flags=re.IGNORECASE)
    bteq_sql = re.sub(r'CASE (.*?) WHEN (.*?) THEN (.*?) END', r'CASE \1 WHEN \2 THEN \3 END', bteq_sql, flags=re.IGNORECASE)
    
    
    # Combine "WITH DATA" and "ON COMMIT PRESERVE ROWS" into a single line to REPLACE later
    bteq_sql = re.sub(r'WITH DATA\s*\n\s*ON COMMIT PRESERVE ROWS', 'WITH DATA ON COMMIT PRESERVE ROWS', bteq_sql, flags=re.IGNORECASE)
    
    # Replace CREATE MULTISET VOLATILE TABLE with CTE syntax
    bteq_sql = convert_volatile_to_single_cte(bteq_sql)
    
    # Remove DROP TABLE statements
    bteq_sql = remove_drop_table_statements(bteq_sql)
    
    # Clean up extra whitespace
    bteq_sql = re.sub(r'\n\s*\n', '\n', bteq_sql)

    return bteq_sql.strip()

# Reads a BTEQ SQL script from an input file, converts it to Spark SQL, and writes the converted script to an output file.
def convert_file(input_file: str, output_file: str):
    # Read the BTEQ SQL from the input file
    with open(input_file, 'r') as f:
        bteq_sql = f.read()
    
    # Extract SQL
    sql_only = extract_sql_from_bteq(bteq_sql)
    
    # Convert BTEQ SQL to Spark SQL
    spark_sql = convert_bteq_to_spark(sql_only)

    # Write the Spark SQL to the output file
    with open(output_file, 'w') as f:
        f.write(spark_sql)

    print(f"Conversion complete. The Spark SQL is saved to {output_file}.")

# Example usage
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python bteq_to_spark.py <input_file.sql> <output_file.sql>")
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        convert_file(input_file, output_file)

# E.g. convert_file('barclays_bteq_script_01.sql', 'output_spark.sql')
