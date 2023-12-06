import json, datetime, psycopg2
from sqlalchemy import create_engine
import pandas as pd

def main(): 

    results = json_into_dict("data/results/22.json")
    quali   = json_into_dict("data/qualifying/22.json")
    drivers = json_into_dict("data/drivers.json")

    #geting a pandas dataframe from the relevant json
    drivers_df = get_drivers_df(drivers["MRData"]["DriverTable"]["Drivers"])
    
    load_into_pg(drivers_df)


def json_into_dict(file_path) -> dict:
    with open(file_path, 'r') as f:
        dict = json.load(f)

    return dict


def get_drivers_df(drivers_lst: list[dict]) -> pd.DataFrame:
    for drivers_dict in drivers_lst:
        drivers_dict['permanentNumber'] = int(drivers_dict['permanentNumber'])
        drivers_dict['dateOfBirth'] = datetime.datetime.strptime(drivers_dict['dateOfBirth'], "%Y-%m-%d").date()
        del drivers_dict['url'], drivers_dict['driverId']

    return pd.DataFrame(drivers_lst)


def load_into_pg(df: pd.DataFrame):
    engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/test")
    conn = psycopg2.connect("dbname=test user=postgres password=password port=5432 host=localhost")
    cur = conn.cursor()
    
    table_query = "CREATE TABLE drivers (perm_number INT, code VARCHAR(3), name VARCHAR(50), last_name VARCHAR(50), date_of_birth DATE, nationality VARCHAR(50));"
    cur.execute(table_query)
    conn.commit()

    #df.to_sql('drivers', engine)
    conn.close()
    cur.close()
    
    
if __name__ == "__main__":
    main()
