import json, datetime
import pandas as pd

def main(): 

    results = json_into_dict("data/results/22.json")
    quali   = json_into_dict("data/qualifying/22.json")
    drivers = json_into_dict("data/drivers.json")

    drivers_lst = convert_types(drivers["MRData"]["DriverTable"]["Drivers"])
    drivers_df = pd.DataFrame(drivers_lst)



def json_into_dict(file_path) -> dict:
    with open(file_path, 'r') as f:
        dict = json.load(f)

    return dict

def convert_types(drivers_lst: list[dict]):
    for drivers_dict in drivers_lst:
        drivers_dict['permanentNumber'] = int(drivers_dict['permanentNumber'])
        drivers_dict['dateOfBirth'] = datetime.datetime.strptime(drivers_dict['dateOfBirth'], "%Y-%m-%d").date()

    return drivers_lst

if __name__ == "__main__":
    main()
