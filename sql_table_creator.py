# This file is used for output generator
def pd2sql(dtype):
    """Function to convert pandas dtype to SQL data type."""
    if "int" in str(dtype):
        return "INT"
    elif "float" in str(dtype):
        return "FLOAT"
    elif "object" in str(dtype):
        return "VARCHAR(255)"
    elif "datetime" in str(dtype):
        return "DATETIME"
    else:
        return "TEXT"


def generate_1nf(primary_keys, df):
    table_name = "_".join(primary_keys) + "_table"
    # Start creating the SQL query
    query = f"CREATE TABLE {table_name} (\n"

    for column, dtype in zip(df.columns, df.dtypes):
        if column in primary_keys:
            query += f"  {column} {pd2sql(dtype)} PRIMARY KEY,\n"
        else:
            query += f"  {column} {pd2sql(dtype)},\n"

    query = query.rstrip(',\n') + "\n);"

    print(query)


def generate_2nf_3nf(relations):
    for relation in relations:
        primary_keys = relation
        table_name = "_".join(relation) + '_table'
        relation = relations[relation]

        query = f"CREATE TABLE {table_name} (\n"

        for column, dtype in zip(relation.columns, relation.dtypes):
            if column in primary_keys:
                query += f"  {column} {pd2sql(dtype)} PRIMARY KEY,\n"
            else:
                query += f"  {column} {pd2sql(dtype)},\n"

        query = query.rstrip(',\n') + "\n);"

        print(query)


def generate_bcnf_4nf_5nf(relations):
    for relation in relations:
        primary_key = relation.columns[0]
        table_name = f'{primary_key}_table'

        query = f"CREATE TABLE {table_name} (\n"

        for column, dtype in zip(relation.columns, relation.dtypes):
            if column == primary_key:
                query += f"  {column} {pd2sql(dtype)} PRIMARY KEY,\n"
            else:
                query += f"  {column} {pd2sql(dtype)},\n"

        query = query.rstrip(',\n') + "\n);"

        print(query)
