from scrapy import cmdline
import os

os.chdir('./_scrapy_mysql_demo/saikr')

cmdline.execute('scrapy crawl sk'.split())
