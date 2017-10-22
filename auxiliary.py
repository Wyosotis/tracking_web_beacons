

#########################################################################################################################

checked_list = []

def parse_log(log, db, db2):
    global checked_list
    
    print(log)
    out = []
    with open(log) as data:
        for line in data:
            begin = line.find('Uri: ')
            if begin != -1:
                end = line.find('\n', begin + 5)
                if end != -1:
                    link = line[begin + 5 : end]
                    out.append(link)

    total = checked = _supposed_beacon_count = _beacon_count = _not_found = 0
    total = len(out)
    
    for imageUrl in out:
        checked += 1
        
        if imageUrl in checked_list:
            continue
        
        print('Check uri: ' + imageUrl) 
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

        if accessible == 1 and bNotImg == False:
            print('Save new entry in databases. Uri: ' + imageUrl)
            logging.info('Save new entry in databases. Uri: ' + imageUrl)
            db.update_method1_data(imageUrl, accessible, size, cookie)
            db2.update_method2_data(imageUrl, accessible, content_length, cookie)

        print('Total checked: {0}/{1}\nBeacons found: {2}\nImages < 100b: {3}\nErrors: {4}'.format(checked, total, _beacon_count, _supposed_beacon_count,
                                                                                               _not_found))

        checked_list.append(imageUrl)             
        

def parse_logs():
    logname = u'tracking_' + str(date.fromtimestamp(time.time())) + '_final' + u'.log'
    print('Log file: ' + logname)
    logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s', level = logging.INFO, filename = logname)

    db = SqlConnector('tracking.db')
    db2 = SqlConnector('tracking2.db')
    
    logs = ['tracking_2017-01-10_images_1.json.log',
            'tracking_2017-01-09_images_8.json.log',
            'tracking_2017-01-08_images_7.json.log',
            'tracking_2017-01-06_images_6.json.log',
            'tracking_2017-01-05_images_5.json.log',
            'tracking_2017-01-05_images_4.json.log',
            'tracking_2017-01-03_images_3.json.log',
            'tracking_2017-01-03_images_2.json.log' ]

    for log in logs:
        parse_log(log, db, db2)


def update_terminal(data):
    print('\b' * 10, end = ' ')
    print(data, end = ' ')
    sys.stdout.flush()
    print()


def not_found(beacon, beacons):
    for item in beacons:
        if beacon[0] == item[0]:
            return False
    return True


g_found = g_total = g_checked = i_counter = 0


def find_relative_complement(_input, beacon):
    logname = _input[0]
    global g_found, g_total, g_checked, i_counter

    bNotFound = not_found(beacon, _input[1])
    
    if bNotFound:
        #logname.write("Uri = {0} w = {1} h = {1}\n".format(beacon[0], beacon[1][0], beacon[1][1]))
        logname.write("Uri = {0} content_length = {1}\n".format(beacon[0], beacon[1]))

    g_lock.acquire()
    g_checked += 1
    i_counter += 1
    if bNotFound:
        g_found += 1
    if i_counter == 300:
        i_counter = 0 
        print("Checked {0}/{1}. The size of symmetric difference of method1 and method2 = {2}".format(g_checked, g_total, g_found))
    g_lock.release()

    
def find_symmetric_difference():
    global g_total
    #logname1 = open(u'relative_complement_of_method1.txt', 'w+')
    logname2 = open(u'relative_complement_of_method2.txt', 'w+')

    db1 = SqlConnector('tracking.db')
    db2 = SqlConnector('tracking2.db')

    beacons1 = [] #beacons (method 1)
    beacons2 = [] #beacons (method 2)

    bound = 100

    beacons = db1.extract_beacons_method1()
    for item in beacons:
        beacons1.append((item[0], (item[1], item[2])))

    g_total = len(beacons1)
    
    beacons = db2.extract_beacons_method2(bound)
    for item in beacons:
        beacons2.append((item[0], item[1]))

    g_total += len(beacons2)

    beacons = [] 

    '''pool = Pool()
    func = partial(find_relative_complement, (logname1, beacons2))  
    pool.map(func, beacons1)
    pool.close()
    pool.join()

    logname1.close()'''

    pool = Pool()
    func = partial(find_relative_complement, (logname2, beacons1))  
    pool.map(func, beacons2)
    pool.close()
    pool.join()

    logname2.close()
    
    print('Done.')

        
#######################################################################################################################################
