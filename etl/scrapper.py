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


def extract_files(url,current_month):
    with RarFile(BytesIO(url.content)) as rf:
        for file in rf.infolist():
            file_name = str(file.filename).split('/')
            if file_name[1].startswith(current_month):
                rf.extract(file.filename,'./files')