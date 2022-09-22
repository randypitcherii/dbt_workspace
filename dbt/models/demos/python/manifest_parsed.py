def model(dbt, session):
    dbt.config(materialized="table")

    df = dbt.ref("my_first_model")
    return df