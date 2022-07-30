import requests

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