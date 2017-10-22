#!/usr/bin/python

from sql_connector import SqlConnector


def plot1(dbname, condition):
    #providers of beacons - count of beacons 
    logname = open(u'plot1'+dbname+'.csv', 'w')
    db = SqlConnector(dbname)

    output = db.execute("SELECT image_domains.domain, count(images.id) \
                as imgcount FROM images INNER JOIN image_domains ON \
                image_domains.id = images.id_image_domains WHERE " + condition +
                " group by image_domains.domain order by imgcount DESC;")

    for item in output:
        logname.write("{0};{1}\n".format(item[0], item[1]));
        print(item)

    logname.close() 


def plot2(dbname, condition):
    #providers of beacons - count of pages
    logname = open(u'plot2'+dbname+'.csv', 'w')
    db = SqlConnector(dbname)

    output = db.execute("SELECT image_domains.domain, count(distinct images.id_pages) \
                as pagescount FROM images INNER JOIN image_domains ON \
                image_domains.id = images.id_image_domains WHERE " + condition +
                " group by image_domains.domain order by pagescount DESC;")

    for item in output:
        logname.write("{0};{1}\n".format(item[0], item[1]));
        print(item)

    logname.close()


def plot3(dbname, condition):
    #users of beacons - % pages with at least 1 beacon
    logname = open(u'plot3'+dbname+'.csv', 'w')
    db = SqlConnector(dbname)

    output = db.execute("SELECT domains.domain, count(distinct pages.id) \
                as pagescount FROM images INNER JOIN pages ON \
                pages.id = images.id_pages INNER JOIN domains ON \
                pages.id_domains = domains.id group by domains.domain order by pagescount DESC;")

    for item in output:
        pages_count = item[1]
        output = db.execute("SELECT count(distinct pages.id) \
                as pagescount FROM images INNER JOIN pages ON \
                pages.id = images.id_pages INNER JOIN domains ON \
                pages.id_domains = domains.id WHERE domains.domain = '" + item[0] +
                "' and " + condition + ";")
        percentage = output[0][0]/item[1]*100
        print("{0} {1} {2} : {3}%\n".format(item[0], item[1], output[0][0], percentage))
        logname.write("{0};{1}\n".format(item[0], percentage))
    logname.close()


def func2(dbname):
    d = {a: 0 for a in range(101)}
    logname = open(u'plot3'+dbname+'.csv', 'r')
    for line in logname:
        percentage = round(float((line.split(";")[1])))
        d[percentage] = d[percentage] + 1
    logname.close()
    logname = open(u'plot3_final'+dbname+'.csv', 'w')
    for i in d:
        logname.write("{0};{1}\n".format(i, d[i]))
    logname.close()
                

def plot4(dbname, condition):
    #providers of beacons - count of domains
    logname = open(u'plot4'+dbname+'.csv', 'w')
    db = SqlConnector(dbname)

    output = db.execute("SELECT image_domains.domain, count(distinct pages.id_domains) \
                as domainscount FROM images INNER JOIN image_domains ON \
                image_domains.id = images.id_image_domains INNER JOIN pages ON \
                images.id_pages = pages.id WHERE " + condition +
                " group by image_domains.domain order by domainscount DESC;")

    for item in output:
        logname.write("{0};{1}\n".format(item[0], item[1]));
        print(item)

    logname.close()


def extract_pixel(line):
    fb_pixel = "null" 
    b = line.find("id=")
    if b != -1:
        e = line.find("&", b)
        if e != -1:
            fb_pixel = line[b+3:e]
    return fb_pixel

                
def extract_facebook_pixels(dbname):
    logp1 = open(u'fb_pixel_pattern1.txt', 'w')
    logp2 = open(u'fb_pixel_pattern2.txt', 'w')
    logp3 = open(u'fb_pixel_pattern3.txt', 'w')
    logp4 = open(u'fb_pixel_pattern4.txt', 'w')
    logp5 = open(u'fb_pixel_pattern5.txt', 'w')

    db = SqlConnector(dbname)
    
    output = db.execute("SELECT images.url, pages.url, images.width, images.height \
                FROM images INNER JOIN pages ON \
                pages.id = images.id_pages INNER JOIN image_domains ON \
                images.id_image_domains = image_domains.id WHERE width<=1 \
                and height<=1 and image_domains.domain = 'facebook.com';")

    fb_unique_pixels1 = []
    fb_unique_pixels2 = []
    fb_unique_pixels3 = []
    fb_unique_pixels4 = []
    fb_unique_pixels5 = []
    fb_unique_pixels3 = []
    count_pages_pattern1 = 0
    count_pages_pattern2 = 0
    count_pages_pattern3 = 0
    count_pages_pattern4 = 0
    count_pages_pattern5 = 0

    
    for item in output:
        
        if item[0].find("/tr/?") != -1:
            count_pages_pattern1 += 1
            fb_pixel = extract_pixel(item[0])
            
            if fb_pixel != "null" and fb_pixel not in fb_unique_pixels1:
                fb_unique_pixels1.append(fb_pixel)
                logp1.write("On page {0} w={1} h={2}\n\t {3}\n\n".format(item[1], item[2], item[3], item[0]));
            
        elif item[0].find("/tr?") != -1:
            count_pages_pattern2 += 1
            fb_pixel = extract_pixel(item[0])
            
            if fb_pixel != "null" and fb_pixel not in fb_unique_pixels2:
                fb_unique_pixels2.append(fb_pixel)
                logp2.write("On page {0} w={1} h={2}\n\t {3}\n\n".format(item[1], item[2], item[3], item[0]));
                
        elif item[0].find("brandlift.php?") != -1:
            count_pages_pattern3 += 1
            fb_pixel = extract_pixel(item[0])
            
            if fb_pixel != "null" and fb_pixel not in fb_unique_pixels3:
                fb_unique_pixels3.append(fb_pixel)
                logp3.write("On page {0} w={1} h={2}\n\t {3}\n\n".format(item[1], item[2], item[3], item[0]));
                
        elif item[0].find("offsite_event.php?") != -1:
            count_pages_pattern4 += 1
            fb_pixel = extract_pixel(item[0])
            
            if fb_pixel != "null" and fb_pixel not in fb_unique_pixels4:
                fb_unique_pixels4.append(fb_pixel)
                logp4.write("On page {0} w={1} h={2}\n\t {3}\n\n".format(item[1], item[2], item[3], item[0]));
        else:
            count_pages_pattern5 += 1
            fb_pixel = "null"
            b = item[0].find("spacer.gif?")
            if b != -1:
                fb_pixel = item[0][b+11:len(item[0])]
                if fb_pixel != "null" and fb_pixel not in fb_unique_pixels5:
                    fb_unique_pixels5.append(fb_pixel)
            logp5.write("On page {0} w={1} h={2}\n\t {3}\n\n".format(item[1], item[2], item[3], item[0]));                
    
    p1 = len(fb_unique_pixels1)
    p2 = len(fb_unique_pixels2)
    p3 = len(fb_unique_pixels3)
    p4 = len(fb_unique_pixels4)
    p5 = len(fb_unique_pixels5)
    
    print('Count of pages with fb pixels [pattern1]: {0}'.format(count_pages_pattern1))
    logp1.write('Count of pages with fb pixels [pattern1]: {0}\n'.format(count_pages_pattern1))
    
    print('Count of pages with fb pixels [pattern2]: {0}'.format(count_pages_pattern2))
    logp2.write('Count of pages with fb pixels [pattern2]: {0}\n'.format(count_pages_pattern2))
    
    print('Count of pages with fb pixels [pattern3]: {0}'.format(count_pages_pattern3))
    logp3.write('Count of pages with fb pixels [pattern3]: {0}\n'.format(count_pages_pattern3))

    print('Count of pages with fb pixels [pattern4]: {0}'.format(count_pages_pattern4))
    logp4.write('Count of pages with fb pixels [pattern4]: {0}\n'.format(count_pages_pattern4))

    print('Count of pages with fb pixels [pattern5]: {0}'.format(count_pages_pattern5))
    logp5.write('Count of pages with fb pixels [pattern5]: {0}\n'.format(count_pages_pattern5))
    
    print('Count of unique fb pixels [pattern1]: {0}'.format(p1))
    logp1.write('Count of unique fb pixels [pattern1]: {0}\n'.format(p1))
    
    print('Count of unique fb pixels [pattern2]: {0}'.format(p2))
    logp2.write('Count of unique fb pixels [pattern2]: {0}\n'.format(p2))
    
    print('Count of unique fb pixels [pattern3]: {0}'.format(p3))
    logp3.write('Count of unique fb pixels [pattern3]: {0}\n'.format(p3))

    print('Count of unique fb pixels [pattern4]: {0}'.format(p4))
    logp4.write('Count of unique fb pixels [pattern4]: {0}\n'.format(p4))

    print('Count of unique fb pixels [pattern5]: {0}'.format(p5))
    logp5.write('Count of unique fb pixels [pattern5]: {0}\n'.format(p5))
    
    print('Total count of uniqie fb pixels: {0}'.format(p1 + p2 + p3 + p4 + p5))
    print('Total count of pages with fb pixels: {0}'.format(count_pages_pattern1 +
                                                                   count_pages_pattern2 +
                                                                   count_pages_pattern3 +
                                                                   count_pages_pattern4 +
                                                                   count_pages_pattern5))

    logp1.close()
    logp2.close()
    logp3.close()
    logp4.close()
    logp5.close()



def extract_twitter_pixels(dbname):
    logp1 = open(u'twitter_pixels.txt', 'w')

    db = SqlConnector(dbname)
    
    output = db.execute("SELECT images.url, pages.url, images.width, images.height \
                FROM images INNER JOIN pages ON \
                pages.id = images.id_pages INNER JOIN image_domains ON \
                images.id_image_domains = image_domains.id WHERE width<=1 \
                and height<=1 and image_domains.domain = 'twitter.com';")

    
    for item in output:
        logp1.write("On page {0} w={1} h={2}\n\t {3}\n\n".format(item[1], item[2], item[3], item[0]));

    logp1.close()


def extract_google_pixels(dbname):
    logp1 = open(u'google_pixels.txt', 'w')

    db = SqlConnector(dbname)
    
    output = db.execute("SELECT images.url, pages.url, images.width, images.height \
                FROM images INNER JOIN pages ON \
                pages.id = images.id_pages INNER JOIN image_domains ON \
                images.id_image_domains = image_domains.id WHERE width<=1 \
                and height<=1 and image_domains.domain = 'google.fr' LIMIT 500;")

    
    for item in output:
        logp1.write("On page {0} w={1} h={2}\n\t {3}\n\n".format(item[1], item[2], item[3], item[0]));

    logp1.close() 
            
        

    
if __name__ == "__main__":
    #plot1('tracking.db', 'width<=1 and height<=1')
    #plot1('tracking2.db', 'content_length <= 100')
    #plot2('tracking.db', 'width<=1 and height<=1')
    #plot2('tracking2.db', 'content_length <= 100')
    #plot3('tracking.db', 'width<=1 and height<=1')
    #func2('tracking.db') 
    #plot3('tracking2.db', 'content_length <= 100')
    #plot4('tracking.db', 'width<=1 and height<=1')
    #plot4('tracking2.db', 'content_length <= 100')
    #extract_twitter_pixels('tracking.db')
    extract_google_pixels('tracking.db')
