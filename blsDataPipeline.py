import requests
import json
import pandas as pd
from datetime import datetime as dt
import time

class API:
    def __init__(self, url, header):
        self.url = url
        self.header = header
        
    def get(url, header):
        response = requests.get(url, headers= header)
        if response.ok == True:
            return response
        else:
            raise Exception("Error in server or client request")
            
    def post(url, payload, header):
        response = requests.post(url, data = payload, headers=header)
        if response.ok == True:
            return response
        else:
            raise Exception("Error in server or client request")
 


class DataProcess():
    
    def run_bls_pipeline(self):
        
        self.fetch_data()
        
        self.mirror_data()
        
        print("*** Pipeline completed ***")

    def fetch_data(self):

        api_context = json.load(open('api_context.json'))
        header = api_context['header']
        payload = json.dumps(api_context['payload']) #### Serializing the request
        url = api_context['url']
        
        reponse_text = API.post(url, payload, header).text  
        reponse = json.loads(reponse_text)  ### Deserializing the json string we got to an object. 
        
        ### Error handling.
        if reponse['status'] == "REQUEST_NOT_PROCESSED":
            raise Exception(reponse['message'][0])

        elif len(reponse) == 0:
            raise Exception("The API Returned no data")
        
        else:
            reponse["received_date"] = str(dt.today())  ### Adding the timestamp when the file is saved.
            reponse["is_processed"] = 0  ### Setting an indication if a file should be processed or not.
            # return API.post(url, payload, header).text
            
            file_name = 'bls_' + str(time.strftime("%Y%m%d")) + '.json'
            with open(file_name, 'w') as file:   ### Writing raw file to storage (in this case localy).
                json.dump(reponse, file)
            print("**** Data fetched & stored successfully ****")

        with open('files_to_mirror.json', 'w') as file:   ### Writing to a json file the file name that we need to process.
            json.dump({"file_name" : [file_name]}, file)


    def mirror_data(self):

        table_data = []   ### Initializing a list to store the rows in it.

        files = json.load(open('files_to_mirror.json')) 
        
        ### Looping through the files we saved from our previous process.
        for file in files['file_name']:
            if len(files['file_name']) == 0:
                raise Exception("*** There are no files to process ***")
            else:
                json_data = json.load(open(file))
                if json_data['is_processed'] == 1:
                    pass
                    # raise Exception("The file :" + file_name + " has been processed already")
                else:
                    for series in json_data['Results']['series']:
                        for row in series['data']:
                            row['seriesID'] = series['seriesID']
                            row['received_date'] = json_data['received_date']
                            table_data.append(row)

                json_data['is_processed'] = 1   ### Changing the indication that the file has been processed.

                with open(file, 'w') as f:   ### Writing the raw file AGAIN to storage to save it's currrent version (is_procssed = 1).
                    json.dump(json_data, f)
                
                
                pd.DataFrame(table_data).astype(str).to_parquet('bls_data', compression='gzip', partition_cols=['seriesID', 'year'])
                print("**** Data from file: " + file + " has been mirrored and stored ****")
        
        with open('files_to_mirror.json', 'w') as file:   ### Writing to a json file the file name that we need to process.
                json.dump({"file_name" : []}, file)
                



