import sqlite3
import random

class SqlConnector():
    def __init__(self, db_name):
        self.db = sqlite3.connect(db_name)
        self.db.text_factory = str
        self.cur = self.db.cursor()
        self.__execute_sql("PRAGMA journal_mode = OFF")

        
    def get_connection(self, db_name):
        return self.db.cursor()


    def close_connection(self):
        self.db.close()


    def load_domain_into_db(self, site):
        try:
            l = site.split('_')
            self.__execute_sql('INSERT into domains(rank, domain) VALUES({0}, "{1}");'.format(l[0], l[1]))
            self.db.commit() 
        except sqlite3.IntegrityError:
            return None
        return self.cur.lastrowid


    def get_images_count(self):
        try:
            self.__execute_sql('select count(id) from images;')
        except sqlite3.Error as e:
            logging.info('sqlite3.Error: {0}'.format(e.args[0]))
            return None 
        return self.cur.fetchone()[0]

    
    def extract_image(self, _id):
        try:
            self.__execute_sql('select * from images where id = {0};'.format(_id))
        except sqlite3.Error as e:
            logging.info('sqlite3.Error: {0}'.format(e.args[0]))
            return None 
        return self.cur.fetchone()


    def extract_beacons_method2(self, x):
        try:
            self.__execute_sql('select url, content_length from images where content_length <= {0};'.format(x))
        except sqlite3.Error as e:
            logging.info('sqlite3.Error: {0}'.format(e.args[0]))
            return None 
        return self.cur.fetchall()


    def extract_beacons_method1(self):
        try:
            self.__execute_sql('select url, width, height from images where width <= 1 and height <= 1;')
        except sqlite3.Error as e:
            logging.info('sqlite3.Error: {0}'.format(e.args[0]))
            return None 
        return self.cur.fetchall()


    def load_page_into_db(self, page, domain_id):
        try:
            self.__execute_sql('INSERT into pages(url, id_domains) VALUES("{0}", {1});'.format(page, domain_id))
            self.db.commit()
        except sqlite3.Error as e:
            logging.info('sqlite3.Error: {0}'.format(e.args[0]))
            return None 
        return self.cur.lastrowid


    def execute(self, sql):
        try:
            self.cur.execute(sql)
        except sqlite3.Error as err:
            logging.info('sqlite3.Error: {0}'.format(e.args[0]))
            return None 
        return self.cur.fetchall()

        
    def __execute_sql(self, sql):
        count = 0 
        while(True):
            try:
                self.cur.execute(sql)
            except sqlite3.OperationalError as err:
                count += 1
                sleep = random.random() + count/100
                if count > 10:
                    print('\033[93msqlite3.Error: {0} Sleep {1} and try again {2} attempt {3}\033[0m'.format(err.args[0], sleep, count, sql))
                time.sleep(sleep)
                continue #db is locked, try again
            except sqlite3.Error as err:
                raise err 
            break
        

    def load_img_domain_into_db(self, domain, page_id):
        try:
            self.__execute_sql('INSERT into image_domains(domain, id_pages) VALUES("{0}", {1});'.format(domain, page_id))
        except sqlite3.IntegrityError:
            self.__execute_sql('select id from image_domains where id_pages={0} limit 1;'.format(page_id))
            return self.cur.fetchone()[0]
        except sqlite3.Error as e:
            logging.info('sqlite3.Error: {0}'.format(e.args[0]))
            print('\033[91msqlite3.Error: {0} {1}\033[0m'.format(err.args[0], sql))
            return None 
        return self.cur.lastrowid


    def update_method1_data(self, imageUrl, accessible, size, cookie):
        try:
            self.__execute_sql('select id from images where url = "{0}" limit 1;'.format(imageUrl))
            res = self.cur.fetchone()
            if res != None: 
                _id = res[0]
                print('Update row with id = {0}'.format(_id))
                logging.info('Update row with id = {0}'.format(_id))
                if size != None:
                    if cookie != None: 
                        self.__execute_sql('update images set accessible = {0}, width = {1}, height = {2}, cookie = "{3}" where id = {4};'.format(accessible, size[0],
                                                                                                                                                size[1], cookie, _id))
                    else:
                        self.__execute_sql('update images set accessible = {0}, width = {1}, height = {2} where id = {3};'.format(accessible, size[0], size[1], _id))
                else:
                    if cookie != None:
                        self.__execute_sql('update images set accessible = {0}, cookie = "{1}" where id = {2};'.format(accessible, cookie, _id))
                    else:
                        self.__execute_sql('update images set accessible = {0} where id = {1};'.format(accessible, _id))
                         
                self.db.commit()
            else:
                logging.info('sqlite3 miss url: {0}'.format(imageUrl))
        except sqlite3.Error as e:
            logging.info('sqlite3.Error: {0}'.format(e.args[0]))


    def update_method2_data(self, imageUrl, accessible, content_length, cookie):
        try:
            self.__execute_sql('select id from images where url = "{0}" limit 1;'.format(imageUrl))
            res = self.cur.fetchone()
            if res != None: 
                _id = res[0]
                print('Update row with id = {0}'.format(_id))
                logging.info('Update row with id = {0}'.format(_id))
                if content_length != None:
                    if cookie != None: 
                        self.__execute_sql('update images set accessible = {0}, content_length = {1}, cookie = "{2}" where id = {3};'.format(accessible, content_length,
                                                                                                                                           cookie, _id))
                    else:
                        self.__execute_sql('update images set accessible = {0}, content_length = {1} where id = {2};'.format(accessible, content_length, _id))
                else:
                    if cookie != None:
                        self.__execute_sql('update images set accessible = {0}, cookie = "{1}" where id = {2};'.format(accessible, cookie, _id))
                    else:
                        self.__execute_sql('update images set accessible = {0} where id = {1};'.format(accessible, _id))
      
                self.db.commit()
            else:
                logging.info('sqlite3 miss url: {0}'.format(imageUrl))
                
        except sqlite3.Error as e:
            logging.info('sqlite3.Error: {0}'.format(e.args[0]))
        
    
    def load_method1_data(self, imageUrl, accessible, size, domain, page_id, cookie):
        domain_id = self.load_img_domain_into_db(domain, page_id)
        if domain_id == None:
            return
        
        try:
            if size != None:
                if cookie != None: 
                    self.__execute_sql('INSERT into images(url, accessible, width, height, id_image_domains, id_pages, cookie)\
                                        VALUES("{0}", {1}, {2}, {3}, {4}, {5}, "{6}");'.format(imageUrl, accessible, size[0], size[1], domain_id,  page_id, cookie))
                else:
                    self.__execute_sql('INSERT into images(url, accessible, width, height, id_image_domains, id_pages)\
                                        VALUES("{0}", {1}, {2}, {3}, {4}, {5});'.format(imageUrl, accessible, size[0], size[1], domain_id,  page_id))
            else:
                if cookie != None:
                    self.__execute_sql('INSERT into images(url, accessible, id_image_domains, id_pages, cookie)\
                                        VALUES("{0}", {1}, {2}, {3}, "{4}");'.format(imageUrl, accessible, domain_id,  page_id, cookie))
                else:
                    self.__execute_sql('INSERT into images(url, accessible, id_image_domains, id_pages)\
                                        VALUES("{0}", {1}, {2}, {3});'.format(imageUrl, accessible, domain_id,  page_id))
                
            self.db.commit()
        except sqlite3.Error as e:
            logging.info('sqlite3.Error: {0}'.format(e.args[0]))
                

    def load_method2_data(self, imageUrl, accessible, content_length, domain,  page_id, cookie):
        domain_id = self.load_img_domain_into_db(domain, page_id)
        
        if domain_id == None:
            return
        
        try:
            if content_length != None:
                if cookie != None:
                    self.__execute_sql('INSERT into images(url, accessible, content_length, id_image_domains, id_pages, cookie)\
                                        VALUES("{0}", {1}, {2}, {3}, {4}, "{5}");'.format(imageUrl, accessible, content_length, domain_id,  page_id, cookie))
                else:
                    self.__execute_sql('INSERT into images(url, accessible, content_length, id_image_domains, id_pages)\
                                        VALUES("{0}", {1}, {2}, {3}, {4});'.format(imageUrl, accessible, content_length, domain_id,  page_id))
            else:
                if cookie != None:
                    self.__execute_sql('INSERT into images(url, accessible, id_image_domains, id_pages, cookie)\
                                        VALUES("{0}", {1}, {2}, {3}, "{4}");'.format(imageUrl, accessible, domain_id,  page_id, cookie))
                else:
                    self.__execute_sql('INSERT into images(url, accessible, id_image_domains, id_pages)\
                                        VALUES("{0}", {1}, {2}, {3});'.format(imageUrl, accessible, domain_id,  page_id))
                self.db.commit()
        except sqlite3.Error as e:
            logging.info('sqlite3.Error: {0}'.format(e.args[0]))
