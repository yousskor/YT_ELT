import requests
import json

import os
from dotenv import load_dotenv 

load_dotenv(dotenv_path="./env")

CHANNEL_HANDLE = 'MrBeast'


def get_paylist_id():
        
        try:
                
            url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            #print(json.dumps(data, indent=4))

            channel_items = data["items"][0]

            channel_playlisID = channel_items["contentDetails"]["relatedPlaylists"]

            print(channel_playlisID)

            return channel_playlisID
        
        except requests.exceptions.RequestException as e:
              raise e
        
if __name__ == "__main__":
      get_paylist_id()


      
              