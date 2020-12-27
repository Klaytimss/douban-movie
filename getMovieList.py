# https://movie.douban.com/j/new_search_subjects?sort=U&range=1,4&tags=&start=3000
import os

import requests
import pandas as pd


proxyHost = "http-dyn.abuyun.com"
proxyPort = "9020"

# 代理隧道验证信息
proxyUser = "HGH5M94830A73AUD"
proxyPass = "F2B9EFF056611594"

proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
    "host": proxyHost,
    "port": proxyPort,
    "user": proxyUser,
    "pass": proxyPass,
}

proxies = {
    "http": proxyMeta,
    "https": proxyMeta,
}

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

# tags ['title', 'rate', 'director', 'casts', 'url', 'tags', 'country', 'release', 'minutes']


def get_movielist(rag):
    movie_list = []
    """获得电影列表"""
    for num in range(1):
        try:
            url = f"https://movie.douban.com/j/new_search_subjects?sort=U&range={rag}&tags=&start={num*20}"
            print(url)
            # 获得20条的记录
            tmp = requests.get(url, headers=headers,timeout=(3,6),proxies=proxies)
            # 转成list
            tmp = tmp.json()['data']
            if tmp:
            # 保存所需要的数据
                for i in tmp:
                    movie_list.append([i['title'],int(float(i['rate']) * 10),i['directors'],i['casts'],i['url'],[],'','',int(0)])
            else:
                break
        except:
            pass
    return movie_list


import numpy as np
indexList = np.arange(9.2, 10, 0.1)
movies = []
for index in indexList:
    with open("count.txt","a+")as F:
        F.write(str(index)[:3])
        F.write("\n")
    movies += get_movielist(f"{index},{index+0.1}")

data = pd.DataFrame(movies,columns=['title', 'rate', 'director', 'casts', 'url', 'tags', 'country', 'release', 'minutes'])

if not os.path.exists('data.csv'):
    data.to_csv("data.csv", index=False, mode="a",encoding="utf-8-sig")
else:
    data.to_csv("data.csv", index=False, mode="a", encoding="utf-8-sig",header=False)



