import json, datetime, psycopg2
from sqlalchemy import create_engine
import pandas as pd

TOP_KEY = "MRData"
RESULTS_ROOT_PATH = "data/results/"

def main(): 

    results_ad = json_into_dict("data/results/22.json")
    quali_ad   = json_into_dict("data/qualifying/22.json")
    drivers = json_into_dict("data/drivers.json")

    #geting a pandas dataframe from the relevant json
    drivers_df = get_drivers_df(drivers[TOP_KEY]["DriverTable"]["Drivers"])

    #load_into_postgres(drivers_df)
    ad_race_dict = results_ad[TOP_KEY]["RaceTable"]["Races"][0]
    #print(drivers_df.head())
    

def json_into_dict(file_path: str) -> dict:
    with open(file_path, 'r') as f:
        dict = json.load(f)

    return dict


def get_drivers_df(drivers_lst: list[dict]) -> pd.DataFrame:
    for drivers_dict in drivers_lst:
        drivers_dict['permanentNumber'] = int(drivers_dict['permanentNumber'])
        drivers_dict['dateOfBirth'] = datetime.datetime.strptime(drivers_dict['dateOfBirth'], "%Y-%m-%d").date()

    df = pd.DataFrame(drivers_lst)
    df.drop(columns=['driverId', 'url'], inplace=True)
    
    df.rename(columns={"permanentNumber": "permanent_number",
                       "givenName": "first_name",
                       "familyName": "last_name",
                       "dateOfBirth": "dob"}, inplace=True)
    
    
    df["Constructor"] = None
    
    race1_dict = json_into_dict(RESULTS_ROOT_PATH+"/1"+".json")
    race1_dict = race1_dict[TOP_KEY]["RaceTable"]["Races"][0]["Results"]
    print(race1_dict)
    
      
    
    return df
    
def load_into_postgres(df: pd.DataFrame):
    engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/test")
    df.to_sql(name='drivers', con=engine)

    
    
if __name__ == "__main__":
    main()
