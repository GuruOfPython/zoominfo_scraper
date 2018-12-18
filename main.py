import multiprocessing as mp
from multiprocessing import Pool, cpu_count
import math
import threading
import csv

from sub_scrapers import *
from url_generator import *
from database_manager import *


RESULT_FILE = None
SUB_LINKS = []
TOTAL_CNT = 0
TOTAL_REQUESTS_CNT = 0
PROXIES = []
TOTAL_DATA = []


def import_proxies():
    try:
        proxy_file_name = 'proxy_http_ip.txt'
        with open(proxy_file_name, 'rb') as text:
            proxies = ["http://" + x.decode("utf-8").strip() for x in text.readlines()]
    except:
        proxies = []
    return proxies

class Zoominfo_Scraper():
    def __init__(self):

        ''' 1. Create a Result file'''
        global RESULT_FILE
        RESULT_FILE = create_result_file(result_file_name='zoominfo.csv')

        ''' 2. Make a multiple-processing pool '''
        cpu_cnt = cpu_count()
        self.pool = mp.Pool(100, maxtasksperchild=150)

    def start_requests(self):
        global START_URLS
        global TOTAL_CNT
        global TOTAL_REQUESTS_CNT
        global PROXIES
        global session
        global TOTAL_DATA

        print('Loading links...')
        START_URLS = open('zoominfo_links.csv', 'r', encoding='utf-8').read().split('\n')
        print('Loaded successfully.')
        main_jobs = []
        for start_url in START_URLS:
            # pxy = random.choice(PROXIES)
            # print(start_url)
            if start_url == '' or 'https://www.zoominfo.com/c/' not in start_url:
                continue
            raw_proxies = import_proxies()
            if raw_proxies:
                PROXIES = raw_proxies

            TOTAL_REQUESTS_CNT += 1
            TOTAL_REQUESTS_CNT = TOTAL_REQUESTS_CNT % 6000
            pxy = PROXIES[TOTAL_REQUESTS_CNT]
            main_job = self.pool.apply_async(get_details, (start_url, pxy, 0, ))
            main_jobs.append(main_job)

        for i, main_job in enumerate(main_jobs):
            result = main_job.get()
            if result[0] == 'Details':
                success_status = result[1]
                retry_num = result[2]
                result_row = result[3]
                url = result[4]
                pxy = result[5]

                if success_status == 'Success':
                    TOTAL_CNT += 1

                    insert_row(result_file=RESULT_FILE, result_row=result_row)
                    print("[Details: {}] ('TOTAL_CNT') => ('{:08d}')".format(pxy, TOTAL_CNT))

                elif success_status == 'Failure' and retry_num < 100:
                    # pxy = random.choice(PROXIES)
                    print('[Details: {}] Failture'.format(pxy))

                    raw_proxies = import_proxies()
                    if raw_proxies:
                        PROXIES = raw_proxies

                    TOTAL_REQUESTS_CNT += 1
                    TOTAL_REQUESTS_CNT = TOTAL_REQUESTS_CNT % 6000
                    pxy = PROXIES[TOTAL_REQUESTS_CNT]
                    main_job = self.pool.apply_async(get_details, (url, pxy, retry_num,))
                    main_jobs.append(main_job)


                


if __name__ == '__main__':
    app = Zoominfo_Scraper()
    app.start_requests()