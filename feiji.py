# encoding: utf-8
import requests
import pymysql.cursors
from bs4 import BeautifulSoup
import re
import queue
import threadpool
import threading

'''返回当前页面的信息列表'''
headers = {
    'Host': 'feijibt.org',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'X-Anit-Forge-Code': '0',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Referer': 'http://feijibt.org/list/ol/1/0/5.html',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cookie': '__cfduid=d81418b3e543479f4e064d1eed3b459091539100930; __guid=17301678.767134759900229100.1539100930295.0957; uv_cookie_115024=1; Hm_lvt_6fb6bee8deeec3134fac140e50fed47a=1539100931; UM_distinctid=16659907571a-0d94ba615cb555-3c604504-1fa400-16659907572118; CNZZDATA1260927344=1149511139-1539098512-null%7C1539098512; Hm_lvt_ffd364edd4473a2f1b73d061ac69854b=1539100932; monitor_count=17; Hm_lpvt_6fb6bee8deeec3134fac140e50fed47a=1539102016; __atuvc=16%7C41; __atuvs=5bbcd1033530cf0800f; Hm_lpvt_ffd364edd4473a2f1b73d061ac69854b=1539102017'
}

def get_conn():
    '''建立数据库连接'''
    conn = pymysql.connect(host='localhost',
                                user='root',
                                password='123456',
                                db='python',
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)
    return conn


def insert(conn, datas):
    '''数据写入数据库'''
    with conn.cursor() as cursor:
        sql = "INSERT IGNORE INTO `feiji_copy` (`magnet_link`, `created_time`,`file_size`,`file_num` , `hot`, `recent_download_time`,`name`, `url`, `key`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, datas)
    conn.commit()


def get_html(page, key_word):
    url = "http://feijibt.org/list/{}/{}/0/2.html".format(key_word, page)
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    html = BeautifulSoup(resp.text.encode(encoding='UTF-8', errors='strict'), "html.parser")
    slist=html.find_all(class_="title")
    content_list = html.find_all(class_="sbar")
    datas_list = []
    for i in range(len(content_list)):
        datas = []
        datas.append(content_list[i].a.attrs['href'].encode('utf-8'))
        span_list = content_list[i].find_all('span')
        created_time = span_list[2].b.contents[0].encode('utf-8')
        datas.append(created_time)
        file_size = span_list[3].b.contents[0].encode('utf-8')
        datas.append(file_size)
        file_num = span_list[4].b.contents[0].encode('utf-8')
        datas.append(int(file_num))
        hot = span_list[5].b.contents[0].encode('utf-8')
        datas.append(int(hot))
        recent_download_time = span_list[6].b.contents[0].encode('utf-8')
        datas.append(recent_download_time)
        h3_a_contents = slist[i + 1].h3.a.contents
        name = ""
        for con in h3_a_contents:
            name += con.string
        datas.append(name)
        url = slist[i + 1].h3.a.attrs['href']
        datas.append(url)
        datas.append(key_word)
        datas_list .append(datas)
    return datas_list

def get_page_count (key_word):
    url = "http://feijibt.org/list/{}/1/0/2.html".format(key_word)
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    html = BeautifulSoup(resp.text.encode(encoding='UTF-8', errors='strict'), "html.parser")
    pager = html.find_all(class_="pager")
    page_count = 0
    if pager is None or len(pager) == 0:
        return page_count
    page_str = pager[0].span.string

    if page_str is not None and len(page_str) > 0:
        page_count = int(re.sub("\D", "", page_str))
    return page_count

def crawl(page, key_word):
    conn = get_conn()  # 建立数据库连接  不存数据库 注释此行
    t = threading.current_thread()
    datas = get_html(page, key_word)
    if datas is None:
        return
    # time.sleep(random.randint(10, 20))
    for row in datas:
        print("data: %s" % row)
        insert(conn, tuple(row))  # 插入数据库，若不想存入 注释此行
    conn.close()



keys = ["电影"]

q = queue.LifoQueue()
for k in keys:
    q.put(k)

pool = threadpool.ThreadPool(3)

def main():
    for key_word in keys :   # 关键字
         page = 1
         page_count = get_page_count(key_word)
         while page < page_count:#
             args = [page, key_word]
             fun_var = [(args, None)]
             requests = threadpool.makeRequests(crawl, fun_var)
             [pool.putRequest(req) for req in requests]
             page += 1

    pool.wait()

if __name__ == '__main__':
    main()