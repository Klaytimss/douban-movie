# 初始化读取ua
import json
import random
import time
import pandas as pd
import requests
from movie_page_parse import MoviePageParse
from multiprocessing.dummy import Pool as ThreadPool
from pymongo import MongoClient

db = {'host': 'localhost', 'port': 27017, 'db':'db', 'collection': 'Douban'}
conn = MongoClient(host=db['host'], port=db['port'])
conn = conn.get_database(db['db'])
conn = conn.get_collection(db['collection'])

proxies = {"https": "https://115.218.183.104:40696"}

headers = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}


def set_random_sleep_time():
    """
    设置随机睡眠时间
    :return:
    """
    # 爬虫间隔时间
    sleep_time = random.randint(1, 2)
    time.sleep(sleep_time)

def set_random_ua():
    """
    设置随机ua
    :return:
    """
    ua_len = len(ua_list)
    rand = random.randint(0, ua_len - 1)
    headers['User-Agent'] = ua_list[rand]
    print('当前ua为' + str(ua_list[rand]))

def read_ip_list():
    """
    读取ip文件
    :return:
    """
    ip_list_file_path = './proxy/ip_list.txt'
    ip_list = []
    with open(ip_list_file_path, 'r') as f:
        line = f.readline()
        while line:
            ip_list.append(line)
            line = f.readline()
    return ip_list

def delete_ip(ip_proxy):
    try:
        a = ip_proxy["https"]
    except:
        a = ip_proxy["http"]

    with open("./proxy/ip_list.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()
    with open("./proxy/ip_list.txt", "w", encoding="utf-8") as f_w:
        for line in lines:
            if a in line:
                print(f"删除{line}")
                continue
            f_w.write(line)

try:
    ua_list_file_path = './proxy/ua_list.txt'
    ua_list = []
    with open(ua_list_file_path, 'r') as f:
        line = f.readline()
        while line:
            ua_list.append(line.strip('\n'))
            line = f.readline()
    print('UA初始化成功')
except Exception as err:
    print('UA初始化失败' + str(err))


def get_movie_info(movie_id):
    print(movie_id)
    """
    获取当前电影信息
    :param movie_id:
    :return:
    """
    # result = conn.find_one({'id': movie_id})
    # if not conn.find_one({'id': movie_id}):
    if True:
        print('开始获取电影' + str(movie_id) + '信息...')
        try:
            set_random_ua()
            get_flag = False
            while not get_flag:
                # set_random_ip()
                movie_url = 'https://movie.douban.com/subject/' + str(movie_id) + '/'

                ip_list = read_ip_list()
                ip_len = len(ip_list)
                rand = random.randint(0, ip_len - 1)
                rand_ip = ip_list[rand]
                if 'https' in rand_ip:
                    proxies = {'https': rand_ip.strip('\n')}
                else:
                    proxies = {'http': rand_ip.strip('\n')}

                try:
                    movie_info_response = requests.get(movie_url, headers=headers, proxies=proxies,timeout=(3,5))
                    set_random_sleep_time()
                    check_ip_status = movie_info_response.status_code
                    print(check_ip_status)
                    if check_ip_status == 200 and "检测到有异常请求从您的IP发出" not in movie_info_response.text:
                        movie_info_html = movie_info_response.text
                        movie_page_parse = MoviePageParse(movie_id, movie_info_html)
                        movie_info_json = movie_page_parse.parse()
                        print('获取电影' + str(movie_id) + '信息成功')
                        print('电影' + str(movie_id) + '信息为' + str(movie_info_json))
                        # print(type(movie_info_json))
                        # 将电影信息保存到文件之中
                        print('保存电影到Mongodb')
                        conn.insert_one(movie_info_json)
                        get_flag = True
                except:
                    delete_ip(proxies)
                    print('当前ip' + str(proxies) + '不可行, 尝试其他中...')

        except Exception as err:
            print('获取电影' + str(movie_id) + '信息失败' + str(err))


# get_movie_info(25927402)
data = pd.read_csv("data.csv")
get_id_list = []
for node in conn.find():
    get_id_list.append(node["id"])

print(len(get_id_list))
print(get_id_list)
data["subId"] = data["url"].apply(lambda x:x.split("/")[-2])
print(data["subId"].nunique())
print(data.groupby("rate").count())
# print(data["subId"])
data = data.query('subId not in @get_id_list')
print(data.shape)
# movie_id_list = data["url"].apply(lambda x:x.split("/")[-2]).tolist()
# movie_id_list.reverse()
# print(movie_id_list)
# movie_pool = ThreadPool(12)
# movie_pool.map(get_movie_info, movie_id_list)
# movie_pool.close()
# movie_pool.join()