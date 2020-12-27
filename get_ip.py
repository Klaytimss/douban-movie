import time
import requests
import json

def get_ip():
    url = 'http://api.xdaili.cn/xdaili-api//privateProxy/applyStaticProxy?spiderId=0de6338b4fb44260a96d1519da4b33e9&returnType=2&count=5'
    response = requests.get(url)
    result = json.loads(response.text)
    ip_list = []
    for res in result['RESULT']:
        ip = res['ip']
        port = res['port']
        ip_port = 'https://' + ip + ":" + port
        print(ip_port)
        ip_list.append(ip_port)


    ip_file_path = './ip_list.txt'
    with open(ip_file_path, 'a+') as f:
        for ip in ip_list:
            f.write(ip + '\n')

if __name__ == '__main__':
    while True:
        try:
            get_ip()
            time.sleep(10)
        except:
            pass
