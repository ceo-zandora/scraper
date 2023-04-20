import re, csv
import requests
from urllib.parse import urlsplit
from collections import deque
from bs4 import BeautifulSoup
import random
import logging
import concurrent.futures
import gspread

gc = gspread.service_account(filename='/root/scraper/creds.json')
sheet = gc.open('Scraper_Database').sheet1

logging.basicConfig(filename='/root/scraper/scraper.log', format='%(asctime)s %(message)s',
                    encoding='utf-8', level=logging.WARNING)

def csvWriter(data):
    sheet.append_row(data)
    

def new_user_agent():
    user_agent_list = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        ]
    for _ in user_agent_list:
        user_agent = random.choice(user_agent_list)
        return user_agent
    

urls = ['https://www.jakolvaelectric.com']
blacklistUrls = []
# Define a list of extensions to check for
extensions = ['.png', '.jpg', '.jpeg', '.gif', 'example.com', 'your-domain.com', 'mysite.com', 'company.com']


with open('/root/scraper/blacklist.txt', 'r', encoding='utf-8') as file:
    blacklistUrls = file.read().splitlines()

def Scrape(detail, retries = 3):
    # ID
    id = ''
    try:
        id = detail['ID']
    except:
        pass

    # Name
    name = ''
    try:
        name = detail['NAME']
    except:
        pass

    # Image
    image = ''
    try:
        image = detail['IMAGE']
    except:
        pass

    # Link
    link = ''
    try:
        link = detail['LINK']
    except:
        pass

    # Phone
    phone = ''
    try:
        phone = detail['PHONE']
    except:
        pass

    # CellPhone
    cellPhone = ''
    try:
        cellPhone = detail['CELL_PHONE']
    except:
        pass

    # Address
    address = ''
    try:
        address = detail['ADDRESS']
    except:
        pass

    # AddressTwo
    addressTwo = ''
    try:
        addressTwo = detail['ADDRESS2']
    except:
        pass

    # Postal Code
    postalCode = ''
    try:
        postalCode = detail['POSTAL_CODE']
    except:
        pass

    # Town
    town = ''
    try:
        town = detail['TOWN']
    except:
        pass

    # Website
    website = ''
    try:
        website = detail['WEBSITE']
    except:
        pass

    # Scraping
    if detail['WEBSITE'] == "":
        Email = ''
    else:
        url = detail['WEBSITE']

        # read url from input
        original_url = url

        # to save urls to be scraped
        unscraped = deque([original_url])

        # to save scraped urls
        scraped = set()

        # to save fetched emails
        emails = set()  

        while len(unscraped):
            url = unscraped.popleft()  
            scraped.add(url)

            parts = urlsplit(url)

            base_url = "{0.scheme}://{0.netloc}".format(parts)
            base = "{0.netloc}".format(parts)

            if "www." in base[0:4]:
                base = base[4:]
            else:
                pass

            #blocking 
            if base in blacklistUrls:
                print(f"Blacklisted domain : {base}")
                break

            if '/' in parts.path:
                path = url[:url.rfind('/')+1]
            else:
                path = url

            #print("Crawling URL %s" % url)

            user_agent = new_user_agent()
            headers = {'User-Agent': user_agent,'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'}

            try:
                response = s.get(url, headers=headers)
                soup = BeautifulSoup(response.text, 'lxml')
                new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", response.text, re.I))
                emails.update(new_emails)

                def href():
                    xyz = []
                    for anchor in soup.find_all("a"):
                        
                        if "href" in anchor.attrs:
                            link = anchor.attrs["href"]
                        else:
                            link = ''

                        if link.startswith('/'):
                            link = base_url + link
                        elif not link.startswith('http'):
                            link = path + '/' + link
                        else:
                            link = link

                        if not link.endswith(".gz"):
                            if base in link:
                                if not link in unscraped and not link in scraped:
                                    xyz.append(link)
                    

                    # if len(xyz) < 25:
                    for x in xyz:
                        unscraped.append(x)


                if url in urls:
                    href()
                else:
                    pass

            
            except Exception as err:
                # Scrape(detail)
                logging.warning(err)
                attempts = 0
                while attempts < retries:
                    try:
                        Scrape()
                        attempts += 1
                    except:
                        pass
                   
        

        clean_email = {item for item in emails if not any(ext in item for ext in extensions)}
        
        if clean_email != "":
            Email = ", ".join(clean_email)
            data = [id, name, image, link, Email, phone, cellPhone, address, addressTwo, postalCode, town, website]
            csvWriter(data=data)
        else:
            Email = detail['EMAIL']





# Open the CSV file
with open('/root/scraper/data.csv') as csv_file:
    # Create a csv reader object
    csv_reader = csv.DictReader(csv_file)

    # Create an empty list to store the dictionaries
    data = []

    # Loop through each row in the CSV file
    for row in csv_reader:
        # Create a dictionary to store the data for this row
        row_dict = {}

        # Loop through each column in the row
        for column, value in row.items():
            # Add the column name and value to the dictionary
            row_dict[column] = value

        # Add the dictionary to the data list
        data.append(row_dict)

for x in range(len(data)):
    if data[x]['WEBSITE'] != '':
        urls.append(str((data[x]['WEBSITE'])).strip())

s = requests.Session()
# print(data[0])
#Scrape(dict({'ID': '7c743bdf-d9ba-4930-9427-0f8fc2d5b02f', 'NAME': 'G.E.S.M.A.', 'IMAGE': 'https://ffessmmedias.blob.core.windows.net/logostructure/Logo-02160007-8c93114b-0351-4c60-823d-4cd7835628c2.png', 'LINK': 'http://plongee-gesma.com', 'EMAIL': '', 'PHONE': '', 'CELL_PHONE': '', 'ADDRESS': 'PISCINE NAUTILIS', 'ADDRESS2': 'RUE DES MESNIERS', 'POSTAL_CODE': '16710', 'TOWN': 'SAINT YRIEIX', 'WEBSITE': 'https://www.jakolvaelectric.com'}))

with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    executor.map(Scrape, data)
