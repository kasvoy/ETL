import requests, json, time

def main():
    #get_all_rounds_2023(suffix="qualifying")
    get_race_schedule()
    

def write_apidata_to_file(url: str, file_path: str):
    with open(file_path, 'w') as file:
        json.dump(requests.get(url).json(), file, indent=4)


#CAUTION - QUERIES API EVERY 2 SECONDS
def get_all_rounds_2023(suffix: str):
    a = input("Sure? This will call the API once per 2 seconds ('y only for yes'): ")

    if a == 'y':
        for round_no in range(22):
            url = f"http://ergast.com/api/f1/2023/{str(round_no+1)}/{suffix}.json"
            f_path = f"data/{suffix}/"+ str(round_no+1)+ ".json"

            write_apidata_to_file(url, f_path)
            time.sleep(2)
    else:
        print("Aborted")
        
def get_drivers_data():
    write_apidata_to_file("http://ergast.com/api/f1/2023/drivers.json", "data/drivers.json")

def get_race_schedule():
    write_apidata_to_file("http://ergast.com/api/f1/2023.json", "data/schedule.json")

if __name__ == "__main__":
    main()