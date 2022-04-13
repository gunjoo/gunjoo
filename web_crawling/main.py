# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path

title_text = []
link_text = []
source_text = []
date_text = []
contents_text = []
result = {}

# 엑셀로 저장하기 위한 변수
RESULT_PATH = "C:/Users/USER/Desktop/crawling_result/"  # 결과 저장할 경로
now = datetime.now()  # 파일이름 현 시간으로 저장하기기

#def daum_search(maxPage, query, s_date, e_date):
def daum_search(maxpage, query, sort, s_date, e_date):
    page = int(maxpage)
    query = query
    s_from = s_date.replace(".", "") + '000000'
    e_to = e_date.replace(".", "") + '235959'

    # 추후 활용 예정
    # 관련도순=0  최신순=1  오래된순=2
    if sort == 0:
        sort = "accuracy"
    elif sort == 1:
        sort = "recency"
    elif sort == 2:
        sort = "old"
    else:
        sort = "accuracy"

    maxPage = page+1
    start = 1

    url_list = []
    media_list = []
    date_list = []
    title_list = []


    while start < maxPage:
        #url = "https://search.daum.net/search?w=news&nil_search=btn&DA=NTB&enc=utf8&cluster=y&cluster_page=1&q=화성시인재육성재단=STC&sort=recentcy&p=1"
        #url = "https://search.daum.net/search?w=news&cluster=n&q=화성시인재육성재단=STC&sort=recency&p=1" #최신순
        #url = "https://search.daum.net/search?w=news&da=pgd&enc=utf8&cluster=y&cluster_page=1&q="+query+"&sort=recency&DA=PGD&p="+str(start)+"&period=u&sd="+s_from+"&ed="+e_to+
        url = "https://search.daum.net/search?w=news&da=pgd&enc=utf8&cluster=y&cluster_page=1&q="+query+"&sort=recency&DA=STC&period=u&sd="+s_from+"&ed="+e_to+"&p="+str(start)
        print('url:', url)
        response = requests.get(url)
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        entires = soup.findAll("a", {"class": "tit_main fn_tit_u"}) #list 형태
        media_dates = soup.findAll("span", {"class": "cont_info"})

        #refactoring 필요
        for entire in entires:
            url_list.append(entire.get('href'))
            title_list.append(entire.getText().strip().replace(',',''))
        for data in media_dates:
            media_date = data.getText().replace(" ", "")
            media_list.append(media_date[:-10])
            date_list.append(media_date[-10:])

        result = {"date": date_list, "title": title_list, "url": url_list, "media": media_list}
        #df = pd.DataFrame.from_dict(result, orient='index') #왼쪽을 column으로 변경
        print('result:', result)

        df = pd.DataFrame(result) #array must all be same length
        start += 1

    df_result = df.drop_duplicates() # 중복제거

    try:
        print('start check_dir')
        Path(RESULT_PATH).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print('>>>check_dir_except:', e)
        pass

    outputFileName = '%s-%s-%s  %s시 %s분 %s초 화성시인재육성재단_언론보도.xlsx' % (
        now.year, now.month, now.day, now.hour, now.minute, now.second)
    df_result.to_excel(RESULT_PATH + outputFileName, sheet_name='sheet1')

    return 0

if __name__ == '__main__':
    nfo_main = input("=" * 50 + "\n" + "입력 형식에 맞게 입력해주세요." + "\n" + " 시작하시려면 Enter를 눌러주세요." + "\n" + "=" * 50)
    maxpage = input("최대 크롤링할 페이지 수 입력하시오: ")
    query = input("검색어 입력: ")
    #sort = input("뉴스 검색 방식 입력(관련도순=0  최신순=1  오래된순=2): ")  # 관련도순=0  최신순=1  오래된순=2
    s_date = input("시작날짜 입력(20xx.xx.xx):")  # 2019.01.04
    e_date = input("끝날짜 입력(20xx.xx.xx):")  # 2019.01.05
    sort = 1 # 추후 사용예정
    daum_search(maxpage, query, sort, s_date, e_date)