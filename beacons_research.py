def calc_best_coeff():
    logname = open(u'out.txt', 'w+')

    db = SqlConnector('tracking.db')
    db2 = SqlConnector('tracking2.db')

    #count = db2.get_images_count()
    count = 100 
    print('Count of images: ', count)

    B = [] #beacons (method 1)


    beacons = db.extract_beacons_method1()
    for item in beacons:
        B.append(item[1]) 

    print('Beacons count: ', len(beacons))

    
    bound = 200

    output = {100: 0, 110: 0, 120: 0, 130: 0, 140: 0, 150: 0, 160: 0, 170: 0, 180: 0, 190: 0, 200: 0}
 
    
    _id = 1
        
    while _id <= count:
        #print('Check {0}/{1}'.format(_id, count))
        
        image = db2.extract_image(_id)
        url = image[1]
        content_length = image[3]

        notInB = False
        
        if content_length != None and content_length != 0:
            if content_length <= bound:
                if url not in B: #supposed beacons which have been detected by method 2 with parameter bound but not with method 1.
                    notInB = True

            if notInB == True:
                print('notInB = True. Check {0}/{1}'.format(_id, count))
                x = 100
                
                while x <= bound:
                    if content_length <= x:
                        output[x] += 1
                            
                    x += 10
            else:
                print('Check {0}/{1}'.format(_id, count))
                
        _id += 1


    print('Done.')
    print(output)
