import requests
from bs4 import BeautifulSoup
from datetime import datetime
from rarfile import RarFile
from io import BytesIO
import os

def get_base_url(base_url,year): 
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')

    try:
        for link in links:      
            if f'Precios {year}' in link.text:
                current_url = link['href']

        return base_url + current_url

    except:
        print("URL not found.")


def ensure_folder(full_path,month):
    # Check if the folder exists
    if not os.path.exists(full_path):
        # Create the folder
        os.makedirs(full_path)
        print(f"Folder '{full_path}' created.")
    else:
        print(f"Folder '{full_path}' already exists.")
    
    return full_path



def extract_files(url,current_month,files_dir):
    with RarFile(BytesIO(url.content)) as rf:
        for file in rf.infolist():
            file_name = os.path.basename(file.filename)

            if file_name.startswith(current_month):
                destination_path = os.path.join(files_dir, file_name)
                with open(destination_path, 'wb') as f:
                    f.write(rf.read(file))
                print(f'File downloaded to {destination_path}')
