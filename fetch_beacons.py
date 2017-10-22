#!/usr/bin/python
#The script for extracting beacons.
#Parse JSON file, get sizes of 3rdParty images, store relations and sizes in database.
#author: Leila Kuntar


import json 
from multiprocessing.dummy import Pool, Lock
from functools import partial
import logging
import time
from datetime import date
import tld
from tld import get_tld
import requests
import random
import sys
from image_parser import ImageParser
from sql_connector import SqlConnector

g_lock = Lock()
g_dic_lock = Lock()



def fetch_url(uri):
    headers = { 
	'User-Agent' : 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
	'Accept-Encoding': 'gzip, deflate, sdch, br',
	'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4',
	'Connection': 'keep-alive',
	'Upgrade-Insecure-Requests': '1',
        } 
   
    err_code = content_length = content_type = status_code = cookie = None
    data = b''

    count = 0 
    while True: 
        try:
            r = requests.get(uri, headers = headers, allow_redirects = True, timeout = 5)
            data = r.content
            status_code = r.status_code
            content_type = r.headers.get('content-type')
            content_length = r.headers.get('content-length')
            cookie = str(requests.utils.dict_from_cookiejar(r.cookies))
            if cookie == '{}':
                cookie = None
            else:
                cookie = cookie[1:-1].replace('"', '')
        except requests.exceptions.ConnectionError as err:
            count += 1
            if count > 4:
                err_code = err
                logging.info('ConnectionError: {0}. Uri: {1}'. format(err, uri))
                break 
            sleep = random.randint(3,8)
            print('\033[91mrequests.exceptions.ConnectionError:\033[0m ' + uri + ' Sleep ' + str(sleep)) 
            time.sleep(sleep)
            continue; 
        except requests.exceptions.RequestException as err:
            err_code = err
            print('\033[91RequestException {0}. Uri: {1}\033[0m'.format(err, uri))
            logging.info('RequestException {0}. Uri: {1}'.format(err, uri))
        break 

    return (data, cookie, content_length, content_type, status_code, err_code)

       
def get_image_size(data, url):
    size = None
    img_format = None 

    try:
        if data != None:
            parser = ImageParser()
            parser.process_data(data)
            size = parser.get_size()
            img_format = parser.get_format()
    except ValueError as err:
        logging.info('ValueError {0}. Uri: {1}'.format(err, url))
        print('\033[91ValueError {0}. Uri: {1}\033[0m'.format(err, url))
        
    return (size, img_format)  


def get_image_domain(url):
    domain = None
    try:
        domain = get_tld(url)
    except tld.exceptions.TldBadUrl:
        logging.info("Can't get domain of {0} tld.exceptions.TldBadUrl".format(url))
    except tld.exceptions.TldDomainNotFound:
        logging.info("Can't get domain of {0} tld.exceptions.TldDomainNotFound".format(url))
        
    return domain


def is_it_3rdparty(url_domen, page):
    if page.find(url_domen) != -1:
        return False
    else:
        return True


g_total_checked = g_beacon_count = g_supposed_beacon_count = g_not_found = g_total_links_count = 0
g_db1_name = g_db2_name = "" 
g_dic = {}


class ImageLink():
    def __init__(self, domain, url, size, cookie, content_length, content_type, accessible):
        self.domain = domain
        self.url = url
        self.size = size
        self.cookie = cookie
        self.content_length = content_length
        self.content_type = content_type
        self.accessible = accessible

    def domain(self):
        return self.domain
    
    def url(self):
        return self.url
    
    def size(self):
        return self.size
    
    def cookie(self):
        return self.cookie
    
    def content_length(self):
        return self.content_length
    
    def content_type(self):
        return self.content_type
    
    def accessible(self):
        return self.accessible
	

def get_status():
    return 'Total checked: {0}/{1}\nBeacons found: {2}\nImages < 100b: {3}\nErrors: {4}'.format(g_total_checked, g_total_links_count,
                                                                                                       g_beacon_count, g_supposed_beacon_count, g_not_found)

    
def parse_page(lst, page):
    global g_total_checked, g_beacon_count, g_supposed_beacon_count, g_not_found, g_db1_name, g_db2_name
    global g_dic, g_dic_lock, g_lock 

    _beacon_count = _supposed_beacon_count = _not_found = 0 
    
    imageUrlList = lst[page]

    links_count = len(imageUrlList)
    
    print('Total img links on page {0} - {1}'.format(page, links_count))

    img_list = []

    for imageUrl in imageUrlList:
        domain = get_image_domain(imageUrl)
                    
        if domain == None:
            logging.info('Error. Not an URL: ' + imageUrl)
            continue #it is not URL, so skip it
                   
        if is_it_3rdparty(domain, page) == False:
            continue #we want analyze only 3rd party images
                    
        (data, cookie, content_length, content_type, status_code, err_code) = fetch_url(imageUrl)
        
        size = img_format = content_format = None

        accessible = 1

        bNotImg = False
        
        while (True):
            if content_type != None and content_type.find('image') != -1:
                content_format = content_type
                
                if content_length != None and int(content_length) < 100: 
                    _supposed_beacon_count += 1
                        
                if status_code == 204:
                    size = (0, 0)
                    _beacon_count += 1 #we suppose that it is a beacon
                    break 
     
            if data != b'':
                (size, img_format) = get_image_size(data, imageUrl)
                
                if img_format == None and content_format == None:
                    bNotImg = True
                    logging.info('Warning! May be not an image : {0} status code: {1} error code: {2}'.format(imageUrl, status_code, err_code) )
                else:
                    if size != None and size[0] == 1 and size[1] == 1:
                        _beacon_count += 1

                break 
                   
            if err_code != None or status_code == 404:
                accessible = 0 
                _not_found += 1
                logging.info("Request error: {0} status code: {1} url: {2}".format(err_code, status_code, imageUrl))

            break

        if bNotImg == False:
            img = ImageLink(domain, imageUrl, size, cookie, content_length, content_type, accessible)
            img_list.append(img)

    g_dic_lock.acquire()
    g_dic[page] = img_list
    g_dic_lock.release()

    g_lock.acquire()
    g_total_checked += links_count
    g_beacon_count += _beacon_count
    g_supposed_beacon_count += _supposed_beacon_count
    g_not_found += _not_found
    g_lock.release()

    print('\033[94m' + get_status() + '\033[0m') 
                      

def get_links_count(sites):
    total = 0 
    for site in sites:
        for page in sites[site]:
            total += len(sites[site][page])
    return total

        
def save_in_database(data):
    db = SqlConnector(g_db1_name)
    db2 = SqlConnector(g_db2_name)

    total = len(data)
    count = 0
    
    for site in data:
        count += 1
        print("\033[92mSave in database: {0}/{1}\033[0m {2}".format(count, total, site))
        domain_id = db.load_domain_into_db(site)
        domain_id2 = db2.load_domain_into_db(site)

        for page in data[site]:
            urlList = data[site][page]
            page_id = db.load_page_into_db(page, domain_id)
            page_id2 = db2.load_page_into_db(page, domain_id2)
   
            for img in urlList:
                if img.accessible == 0:
                    db2.load_method2_data(img.url, img.accessible, img.content_length, img.domain,  page_id2, img.cookie)
                    db.load_method1_data(img.url, img.accessible, img.size, img.domain, page_id, img.cookie)
                else:
                    if img.content_type != None and img.content_type.find('image') != -1:
                        #store image URLs with any content_length
                        db2.load_method2_data(img.url, img.accessible, img.content_length, img.domain,  page_id2, img.cookie)
                        
                    #store image URLs with any size
                    db.load_method1_data(img.url, img.accessible, img.size, img.domain, page_id, img.cookie) 

        
def parse_json_file(file_json):
    global g_total_links_count, g_db1_name, g_db2_name
    global g_dic 

    output = {}
    
    with open(file_json) as json_data:
        sites = json.load(json_data)
        g_total_links_count = get_links_count(sites)
        
        for site in sites:
            g_dic = {}
            pool = Pool()
            func = partial(parse_page, sites[site])  
            pool.map(func, sites[site])
            pool.close()
            pool.join()
            output[site] = g_dic

        return output


def test():
    imageList = ['http://x.bidswitch.net/sync?dsp_id=555&user_id=88886218581760772777J&expires=10',
                 'http://px.owneriq.net/cm?id=&esi=1&google_gid=CAESEH4PqpAHh_HtrWl-e92hq_4&google_cver=1&google_ula=1174,0',
                 'http://px.owneriq.net/cm?id=&esi=1&google_gid=CAESEA6eYwhmVAeFiV5OwnnK8qE&google_cver=1&google_ula=1174,0',
                 'http://px.owneriq.net/cm?id=&esi=1&google_gid=CAESEBkIoUG_iwaGQ3z-fbXJLL8&google_cver=1&google_ula=1174,0',
                 'http://px.owneriq.net/cm?id=&esi=1&google_gid=CAESEACkrx-DS2QTXJNsEintU-c&google_cver=1&google_ula=1174,0',
                 'http://px.owneriq.net/cm?id=&esi=1&google_gid=CAESEGF3otmAbOTRzIXOpP9N6Js&google_cver=1&google_ula=1174,0',
                 'http://px.owneriq.net/cm?id=&esi=1&google_gid=CAESEB7_psRbalJlDDmtJZOy9AA&google_cver=1&google_ula=1174,0',
                 'http://ums.adtechus.com/mapuser?providerid=1044;cfp=1;rndc=1480335468;userid=Q5336218581760772777J',
                 'http://ums.adtechus.com/mapuser?providerid=1044;cfp=1;rndc=1480335462;userid=Q5336218321045151984J',
                 'https://i.stack.imgur.com/tLovi.jpg?s=328&g=1',
                 'http://cdn-a.production.liputan6.static6.com/assets/fonts/bintang/roboto-medium/Roboto-Medium.svg#9e4baab57ed09c2dd1f3a56f2a128453',
                 'https://sync.adaptv.advertising.com/sync?type=gif&key=invitemedianewyork2&uid=CAESEKThGAwerjTswJVyRs_K5Jo&google_cver=1',
                 'https://cm.g.doubleclick.net/pixel?google_nid=mediamath&google_cm&google_hm=UgdYPDDPRH6bpJCBPPLT9A',
                 'http://ubmcmm.baidustatic.com/media/v1/0f000aUSmS9DWAFgM1wfas.jpg']
    
    for imageUrl in imageList:
        domain = get_image_domain(imageUrl)

        if domain == None:
            print('Error. Not an URL: ' + imageUrl)
            continue

        (data, cookie, content_length, content_type, status_code, err_code) = fetch_url(imageUrl)
        size = None
        img_format = None
        
        if data != b'':
            (size, img_format) = get_image_size(data, imageUrl)

        print('Url: {0}\nContent type: {1}\nContent length: {2}\nSize: {3}\nFormat: {4}\nStatus code: {5}\nError code: {6}\nCookie: {7}\n'.format(imageUrl, content_type,
                                                                                                                                   content_length, size,
                                                                                                                                   img_format, status_code,
                                                                                                                                   err_code, cookie))

                
def main():
    global g_db1_name, g_db2_name
    
    argc = len(sys.argv) 
    if argc < 2:
        print("Input JSON file is required")
        print("Examples of usage: \n{0} [data.json] [tracking.db] [tracking2.db] \n{1} [data.json]". format(sys.argv[0], sys.argv[0]))
    else:
        logname = u'tracking_' + str(date.fromtimestamp(time.time())) + '_' + sys.argv[1] + u'.log'
        print('Log file: ' + logname)
        logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s', level = logging.INFO, filename = logname)

        if argc == 4:
            g_db1_name  = sys.argv[2]
            g_db2_name = sys.argv[3]
        else:
            g_db1_name  = 'tracking_' + sys.argv[1] + '.db' 
            g_db2_name = 'tracking2_' + sys.argv[1] + '.db'
            print('Create databases')
            create_db(g_db1_name)
            create_db2(g_db2_name)

        print('Database names: {0}, {1} '.format(g_db1_name, g_db2_name))
        logging.info('Input JSON: {0}'.format(sys.argv[1]))
        logging.info('Database names: {0}, {1} '.format(g_db1_name, g_db2_name))
        
        output = parse_json_file(sys.argv[1])
        save_in_database(output)
        status = get_status()
        print('\033[94m' + status + '\033[0m')
        logging.info(status) 
        
        print('\033[92mDone.\033[0m')


if __name__ == "__main__":
    main()
