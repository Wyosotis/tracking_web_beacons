import sqlite3

def create_common_tables(cur):
    try:
        cur.execute("DROP TABLE IF EXISTS domains;")
        cur.execute("CREATE TABLE domains(\
                       id INTEGER PRIMARY KEY   AUTOINCREMENT,\
                       rank          INT,\
                       domain        VARCHAR(256) UNIQUE NOT NULL);")
        cur.execute("DROP TABLE IF EXISTS pages;")
        cur.execute("CREATE TABLE pages(\
                       id INTEGER PRIMARY KEY  AUTOINCREMENT,\
                       url VARCHAR(2048) NOT NULL,\
                       id_domains INTEGER,\
                       FOREIGN KEY(id_domains) REFERENCES domains(id));")
        cur.execute("DROP TABLE IF EXISTS image_domains;")
        cur.execute("CREATE TABLE image_domains(\
                       id INTEGER PRIMARY KEY  AUTOINCREMENT,\
                       domain VARCHAR(256) NOT NULL,\
                       id_pages INTEGER UNIQUE,\
                       FOREIGN KEY(id_pages) REFERENCES pages(id));")
    except sqlite3.Error as err:
        raise err


def create_db(db_name):
    try:
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        
        create_common_tables(cur)

        cur.execute("DROP TABLE IF EXISTS images;")
        cur.execute("CREATE TABLE images (\
                       id INTEGER PRIMARY KEY  AUTOINCREMENT,\
                       url VARCHAR(2048) NOT NULL,\
                       accessible INTEGER NOT NULL,\
                       width INTEGER,\
                       height INTEGER,\
                       id_image_domains INTEGER,\
                       id_pages INTEGER,\
                       cookie VARCHAR(2048),\
                       FOREIGN KEY(id_pages) REFERENCES pages(id)\
                       FOREIGN KEY(id_image_domains) REFERENCES image_domains(id));")
        db.commit()
        db.close()
    except sqlite3.Error as e:
        print("sqlite3 error %s:" % e.args[0]) 
        sys.exit(1)


def create_db2(db_name):
    try:
        db = sqlite3.connect(db_name)
        cur = db.cursor()

        create_common_tables(cur) 

        cur.execute("DROP TABLE IF EXISTS images;")
        cur.execute("CREATE TABLE images (\
                       id INTEGER PRIMARY KEY  AUTOINCREMENT,\
                       url VARCHAR(2048) NOT NULL,\
                       accessible INTEGER NOT NULL,\
                       content_length INTEGER,\
                       id_image_domains INTEGER,\
                       id_pages INTEGER,\
                       cookie VARCHAR(2048),\
                       FOREIGN KEY(id_pages) REFERENCES pages(id)\
                       FOREIGN KEY(id_image_domains) REFERENCES image_domains(id));")
        db.commit()
        db.close() 
    except sqlite3.Error as e:
        print("sqlite3 error %s:" % e.args[0]) 
        sys.exit(1)



if __name__ == "__main__":
    create_db()
    create_db2() 
