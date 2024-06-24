def insert_df(df):
    columns = ', '.join([f'"{col}"' for col in df.columns])
    values = ', '.join([f'("{val}")' for val in df.values.flatten()])
    insert_query = f"""
    INSERT INTO new_table ({columns})
    VALUES ({values});
    """
    return insert_query
