import requests
from bs4 import BeautifulSoup
from datetime import datetime
from rarfile import RarFile
from io import BytesIO

def getBaseURL(base_url,year): 
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


def extractFiles(url,current_month):
    with RarFile(BytesIO(url.content)) as rf:
        for file in rf.infolist():
            file_name = str(file.filename).split('/')
            if file_name[1].startswith(current_month):
                rf.extract(file.filename,'./files')

base_url = 'https://datos.profeco.gob.mx/datos_abiertos/'
year = str(datetime.now().year)
current_month = str(datetime.now().month -1).rjust(2,'0')
qqp_url = getBaseURL(base_url,year)




url = requests.get(qqp_url)
extractFiles(url,current_month)
###Note
## run this commands in container sudo apt-get update & sudo apt-get install unrar



