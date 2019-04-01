import sys
import csv
import string
import requests
import pymysql
import random
from bs4 import BeautifulSoup
from data import *

reload(sys)
sys.setdefaultencoding('utf-8')

class Crawler:
    def __init__(self, soup):
        self.soup = soup
        self.item = {'data_expose_id':'', # bigint
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

    def items(self):
        prev = 0
        i = 1
        while True:
            ## get data_expose_id
            parse = self.soup.select('#_search_list > div.search_list.basis > ul > li:nth-child(' + str(i) + ')')
            for p in parse:
                self.item['data_expose_id'] = p.get('data-expose-id')
                break

            if self.isNull():
                pass
            if self.isDuplicate(prev):
                break

            ## get price
            parse = self.soup.select('#_search_list > div.search_list.basis > ul > li:nth-child(' + str(i) + ') > div.info > span.price > em > span.num._price_reload')
            for p in parse:
                self.item['price'] = self.PriceEditor(p.text.strip())
                break

            ## get productname
            parse = self.soup.select('#_search_list > div.search_list.basis > ul > li:nth-child(' + str(i) + ') > div.info > a')
            for p in parse:
                self.item['productname'] = p.text.strip()
                break

            ## get category
            parse = self.soup.select('#_search_list > div.search_list.basis > ul > li:nth-child(' + str(i) + ') > div.info > span.depth > a')
            cat = ""
            for p in parse:
                cat += (p.text + ">")
            self.item['category'] = cat[:-1]

            ## get shippingfee
            parse = self.soup.select('#_search_list > div.search_list.basis > ul > li:nth-child(' + str(i) + ') > div.info_mall > ul > li:nth-child(2) > em')
            for p in parse:
                text = p.text.strip()
                if len(text) == 6:    #free shipping
                    self.item['shippingfee'] = 0
                elif len(text) > 0:    #get shipping fee only when it existing
                    self.item['shippingfee'] = self.PriceEditor(text[4:len(text)-1])
                break

            ## get seller
            parse = self.soup.select('#_search_list > div.search_list.basis > ul > li:nth-child(' + str(i) + ') > div.info_mall > p > a.btn_detail._btn_mall_detail')
            for p in parse:
                self.item['seller'] = p.get('data-mall-name').strip()
                break

            ## get reviews
            parse = self.soup.select('#_search_list > div.search_list.basis > ul > li:nth-child(' + str(i) + ') > div.info > span.etc > a.graph > em')
            for p in parse:
                self.item['reviews'] = p.text.strip()
                break

            ## get brand_id, productnumber
            namelist = []
            namestring = ''

            # remove unicodes and make a list
            transTable = string.maketrans("[](),/", "      ")            
            namestring = namestring.translate(transTable)
            namelist = namestring.split()
            
            pids = [] # product number candidates
            for name in namelist:
                large = '' # converted 'name' to get brand id from database
                if len(name) < 2:
                    pass
                elif len(name) > 2:
                    if ord(name[0]) == 54644 and ord(name[1]) == 50808:
                        name = name[2:]
                checker = 0 
                for n in name:
                    code = ord(n)
                    if (48 <= code and code <= 57) or (65 <= code and code <= 90): # if n is number or large alphabet
                        pass
                    elif n == '-' or n == '(' or n == ')':
                        pass
                    elif 97 <= code and code <= 122: # if n is small alphabet
                        code -= 32 # make it large
                    else:
                        checker += 1 # if word contains invalid type letter, add 1
                    try:
                        large += chr(code)
                    except:
                        large += n

                # get brandid and pids(product numbers)
                if len(large) > 1:
                    token = 0
                    for ppap in MMdb.getBrandID(large):
                        if ppap == 0 and checker == 0: # if name looks like product number, append to pids list
                            pids.append(name)
                        if ppap == 1:
                            token += 1
                        elif token == 1:
                            self.item['brand_id'] = ppap

            # convert pids(list) to productnumber(string)
            productnumber = ''
            for pid in pids:
                productnumber += pid + "/"
            self.item['productnumber'] = productnumber[:-1]
            
            yield self.item
            prev = self.item['data_expose_id']
            i += 1
            self.resetItem()

    # if data is invalid value, return True
    def isNull(self):
        data = self.item['data_expose_id']
        if data == '' or data == None:
            return True
        else:
            return False

    # if data is duplicate, it means end-of-page
    def isDuplicate(self, prev):
        if prev == 0: # crawling just started
            return False
        elif prev == self.item['data_expose_id']: # duplicated
            return True
        else:
            return False

    # clear self.item data
    def resetItem(self):
        self.item = {'data_expose_id':'', # bigint
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


    # get price as number. When price is invalid value, return 1
    def PriceEditor(self, price):
        try:
            result = ""
            for p in price:
                try:
                    if 0 <= int(p) and int(p) < 10:
                        result += str(p)
                except:
                    pass

            if len(result) == 0:
                return 1
            else:
                return int(result)
        except:
            return 1

## get full urls as list from file ##
def GetUrls():
    urls = []
    with open(URL_FILE_PATH, 'r') as f:
        reader = csv.reader(f)
        for row in reader:

            for i in range(1, LAST_PAGE):
                url = row[0] + str(i)
                urls.append(url)
    return urls

def main():
    for url in GetUrls():
        headers['User-Agent'] = useragents[random.randrange(0,4)]  # use random user agent
        html = requests.get(url, headers=headers).text
        soup = BeautifulSoup(html, 'html.parser')
        crawler = Crawler(soup)
        for item in crawler.items():
            ESdb.updateItem(item)         

if __name__=="__main__":
    main()

