import pymysql

# ES SERVER MYSQL CONNECTION INFORMATION
ES_DBHOST = ''
ES_USERID = ''
ES_PASSWD = ''
ES_DBNAME = ''

# MM SERVER MYSQL CONNECTION INFORMATION
MM_DBHOST = ''
MM_USERID = ''
MM_PASSWD = ''
MM_DBNAME = ''

# SET PARAMETERS
LAST_PAGE = 50
#LAST_ITEM = 50
URL_FILE_PATH = '/usr/ScrapyProject/data/urls.csv'

# WEB ACCESS HEADERS; user-agent string
headers = {
    'User-Agent': ''
}

EMPTY_ITEM = {
'data_expose_id':'', # bigint
'brand_id':'', # int
'productnumber':'', # char
'price':'', # float
'shippingfee':'', # int
'productname':'', # char
'category':'', # char
'seller':'', # char
'reviews':'', # smallint
'picks':'' # smallint
}

class DBconnector:
    def __init__(self, dbhost, userid, passwd, dbname):
        self.dbhost = dbhost
        self.userid = userid
        self.passwd = passwd
        self.dbname = dbname
        self.mysql = pymysql.connect(host = self.dbhost,
                                user = self.userid,
                                password = self.passwd,
                                db = self.dbname,
                                charset = 'utf8')

    # get brand id from database by brandname
    def getBrandID(self, brandname):
        with self.mysql.cursor() as cursor:
            try:
                cursor.execute("SELECT COUNT(*),id FROM s_articles_supplier_kr WHERE name=\'" + str(brandname) + "\' OR korname=\'" + str(brandname) + "\';")
                answer = cursor.fetchall()
                yield answer[0][0]
                yield answer[0][1]
            except Exception as err:
                print ("getBrandID() Error: {}".format(err))
                yield 0

    # update item to database
    def updateItem(self, item):
        mysql = self.mysql
        with mysql.cursor() as cursor:
            try:
                cursor.execute("SELECT COUNT(*) FROM a_NaverShopping WHERE data_expose_id=" + item['data_expose_id'] + ";")
                if int(cursor.fetchall()[0][0]) > 0: # if item already exists, delete before insert    
                    cursor.execute("DELETE FROM a_NaverShopping WHERE data_expose_id=" + item['data_expose_id'] + ";")
                values = (item['data_expose_id'], item['price'], item['shippingfee'], item['productname'], item['category'], item['seller'], item['reviews'], item['brand_id'], item['productnumber'])
                cursor.execute("INSERT INTO a_NaverShopping(data_expose_id, price, shippingfee, productname, category, seller, reviews, brand_id, productnumber) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);", values) # insert item to mysql
                mysql.commit()                
            except Exception as err:
                print str(item['data_expose_id']) + ("updateItem() Error: {}".format(err))

    def getBrands(self):
        with self.mysql.cursor() as cursor:
            try:
                cursor.execute("SELECT * FROM s_articles_supplier_kr")
                result = cursor.fetchall()
                return result
            except Exception as err:
                print ("getBrandID() Error: {}".format(err))

# mysql connection settings
ESdb = DBconnector(ES_DBHOST, ES_USERID, ES_PASSWD, ES_DBNAME)
MMdb = DBconnector(MM_DBHOST, MM_USERID, MM_PASSWD, MM_DBNAME)
