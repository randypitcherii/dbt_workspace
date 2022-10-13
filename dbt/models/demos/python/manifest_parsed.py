import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table")

    df_zodiac = dbt.ref('zodiac').to_pandas()

    # explode the rows with daily resolution
    df_zodiac['DATE_DAY'] = pd.to_datetime(df_zodiac['DATE_DAY'])

    return df_zodiac