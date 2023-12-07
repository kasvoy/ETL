import json, datetime, psycopg2
from sqlalchemy import create_engine
import pandas as pd

TOP_KEY = "MRData"
RESULTS_ROOT_PATH = "data/results/"

def main(): 

    drivers_dict = json_into_dict("data/drivers.json")
    drivers_df   = get_drivers_df(drivers_dict[TOP_KEY]["DriverTable"]["Drivers"])

    load_into_postgres(drivers_df)


def json_into_dict(file_path: str) -> dict:
    with open(file_path, 'r') as f:
        dict = json.load(f)

    return dict


def get_drivers_df(drivers_lst: list[dict]) -> pd.DataFrame:
    
    #Ensure correct datatypes before loading into Postgress
    for drivers_dict in drivers_lst:
        drivers_dict['permanentNumber'] = int(drivers_dict['permanentNumber'])
        drivers_dict['dateOfBirth'] = datetime.datetime.strptime(drivers_dict['dateOfBirth'], "%Y-%m-%d").date()

    df = pd.DataFrame(drivers_lst)
    df.drop(columns=['driverId', 'url'], inplace=True)
    
    df.rename(columns={"permanentNumber": "permanent_number",
                       "givenName": "first_name",
                       "familyName": "last_name",
                       "dateOfBirth": "dob"}, inplace=True)
    
    
    #As there is no constructor information in the Drivers json file, initiate all to None
    df["constructor"] = None      
    
    round_no = 1

    #Append constructor information from race results until all Constructor info filled
    while df["constructor"].isna().any():
        
        round_no_race_dict = get_race_dict(round_no)
        round_no_race_results: list = round_no_race_dict["Results"]
        
        for driver_info in round_no_race_results:
            driver_dict      = driver_info["Driver"]
            constructor_dict = driver_info["Constructor"]
            
            constructor = constructor_dict['name']
            code        = driver_dict["code"]
            
            df.loc[df['code']==code, "constructor"] = constructor
        
        round_no += 1    
    
    return df
    
    
def load_into_postgres(df: pd.DataFrame):
    
    engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/test")
    df.to_sql(name='drivers', con=engine)


def get_race_dict(round_no: int) -> dict:
    
    file_suffix = f"/{str(round_no)}.json"
    raw_dict = json_into_dict(RESULTS_ROOT_PATH + file_suffix)
    
    return raw_dict[TOP_KEY]["RaceTable"]["Races"][0]
        
    
if __name__ == "__main__":
    main()
