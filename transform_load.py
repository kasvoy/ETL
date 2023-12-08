import json, datetime, psycopg2
from sqlalchemy import create_engine
import pandas as pd

TOP_KEY = "MRData"
RESULTS_ROOT_PATH = "data/results/"
QUALI_ROOT_PATH = "data/qualifying/"


def main(): 

    drivers_dict = json_into_dict("data/drivers.json")
    drivers_df   = get_drivers_df(drivers_dict[TOP_KEY]["DriverTable"]["Drivers"])
    
    race_df = get_race_df()
    quali_df = get_quali_df()
    #load_into_postgres(df=drivers_df, table_name="drivers")
    #load_into_postgres(df=race_df, table_name="races")
    #load_into_postgres(df=quali_df, table_name="qualifying")


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


def get_race_df() -> pd.DataFrame:
    
    dflist: list[pd.DataFrame] = []
    
    for round_no in range(get_total_rounds()):
        race_dict = get_race_dict(round_no=round_no+1)
        results: list[dict] = race_dict["Results"]

        (race_name, circuit_name, race_date) = get_basic_race_info(race_dict)
        no_laps = results[0]["laps"]
        
        fastest_lap_time = None
        fastest_lap_driver_code = None
        
        for driver_info in results:
            if 'FastestLap' in driver_info.keys():
                if driver_info['FastestLap']['rank'] == '1':
                    fastest_lap_time = driver_info['FastestLap']['Time']['time']
                    fastest_lap_driver_code = driver_info['Driver']['code']
                    
                    
        fastest_lap_df = pd.DataFrame([[fastest_lap_time, fastest_lap_driver_code]], columns=["fastest_lap_time", "fastest_lap_driver"])
        info_df = pd.DataFrame([[race_name, circuit_name, race_date, no_laps]], columns=["race_name", "circuit_name", "date", "no_laps"])
        pos_df = get_finishing_pos_df(results)
    
        dflist.append(pd.concat([info_df, pos_df, fastest_lap_df], axis=1))
    
    return pd.concat([df for df in dflist], ignore_index=True)


def get_quali_df() -> pd.DataFrame:
    quali_df_list = []
    
    for round_no in range(get_total_rounds()):
        quali_dict = get_race_dict(round_no=round_no+1, quali=True)
        quali_results: list[dict] = quali_dict["QualifyingResults"]

        
        (race_name, circuit_name, race_date) = get_basic_race_info(quali_dict)
        info_df = pd.DataFrame([[race_name, circuit_name, race_date]], columns=["race_name", "circuit_name", "date"])
        quali_pos_df = get_finishing_pos_df(quali_results)
        
        print(race_name)
        for driver_info in quali_results:
            print(driver_info.keys())
        
        quali_df_list.append(pd.concat([info_df, quali_pos_df], axis=1))
        
    
    return pd.concat([df for df in quali_df_list], ignore_index=True)


def load_into_postgres(df: pd.DataFrame, table_name: str):
    
    engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/test")
    df.to_sql(name=table_name, con=engine, index=False)


def get_race_dict(round_no: int, quali=False) -> dict:
    
    file_suffix = f"{str(round_no)}.json"
    root_path = RESULTS_ROOT_PATH if not quali else QUALI_ROOT_PATH
    raw_dict = json_into_dict(root_path + file_suffix)
    
    return raw_dict[TOP_KEY]["RaceTable"]["Races"][0]

def get_total_rounds():
    schedule_dict = json_into_dict("data/schedule.json")
    return int(schedule_dict[TOP_KEY]["total"])

def get_basic_race_info(race_dict):
    race_name    = race_dict["raceName"]
    circuit_name = race_dict["Circuit"]["circuitName"]
    race_date    = race_dict["date"]
    
    return (race_name, circuit_name, race_date)

def get_finishing_pos_df(results_dict):
    codes = [results_dict[i]['Driver']['code'] for i in range(len(results_dict))]
    return pd.DataFrame([codes], columns=['P'+str(i+1) for i in range(len(results_dict))])

if __name__ == "__main__":
    main()
