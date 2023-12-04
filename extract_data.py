import requests, json, time

def main():
    #get_all_rounds_2023(suffix="qualifying")
    #write_apidata_to_file("http://ergast.com/api/f1/2023/drivers.json", "data/drivers.json")
    pass

def write_apidata_to_file(url: str, file_path: str):
    with open(file_path, 'w') as file:
        json.dump(requests.get(url).json(), file, indent=4)


#CAUTION - QUERIES API EVERY 2 SECONDS
def get_all_rounds_2023(suffix: str):
    input("Sure? This will call the API once per 2 seconds (enter)")

    for round_no in range(22):
        url = f"http://ergast.com/api/f1/2023/{str(round_no+1)}/{suffix}.json"
        f_path = f"files/{suffix}/"+ str(round_no+1)+ ".json"

        write_apidata_to_file(url, f_path)
        time.sleep(2)



if __name__ == "__main__":
    main()