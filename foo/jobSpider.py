import re
import time
import requests
from bs4 import BeautifulSoup
from foo.utils.dbUtil import MysqlObject
from foo.db_config import *


# 定义job模型
class Job:

    def __init__(self, job_name, job_company, job_city, job_salary, job_date):
        self.job_name = job_name
        self.job_company = job_company
        self.job_city = job_city
        self.job_salary = job_salary
        self.job_date = job_date


# 请求页面，获取页面
def get_html(url, data):
    headers = {
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"
    }
    response = requests.get(url, params=data, headers=headers)
    response.encoding = "gbk"
    html = response.text
    soup = BeautifulSoup(html, "lxml")
    return soup


# 解析数据
def parse_html(soup):
    soup = soup.find(attrs={"id": "resultList"})
    title = soup.find(class_="el title")
    soup = soup.select(".el")
    job_list = []
    for job in soup:
        if title != job:
            job_name = job.find(attrs={"class": "t1"}).get_text(strip=True)
            job_company = job.find(attrs={"class": "t2"}).get_text(strip=True)
            job_city = job.find(attrs={"class": "t3"}).get_text(strip=True)
            job_salary = job.find(attrs={"class": "t4"}).get_text(strip=True)
            job_date = job.find(attrs={"class": "t5"}).get_text(strip=True)
            the_job = Job(job_name, job_company, job_city, job_salary, job_date)
            job_list.append(the_job)
    return job_list


# 保存数据
def save_file(job):
    join_str = " " * 5
    seq = (job.job_name, job.job_company, job.job_city, job.job_salary, job.job_date)
    job_message = join_str.join(seq)
    file_object = open('..\\files\\51job.txt', 'a', encoding="utf-8")
    try:
        file_object.write(job_message+"\n")
    finally:
        file_object.close()


# 保存到数据库
def save_database(job, my_database):
    job_name = job.job_name
    job_company = job.job_company
    job_city = job.job_city
    job_area = ""
    if "-" in job_city:
        city = job_city.split("-")
        job_city = city[0]
        job_area = city[1]
    job_salary = job.job_salary
    job_date = job.job_date
    # print(job_name+"\t"+job_company+"\t"+job_city+"\t"+job_area+"\t"+job_salary+"\t"+job_date)
    # 先查询是否已存在数据
    select_sql = '''select * from JOB_DETAIL where JOB_NAME = %s and JOB_COMPANY = %s and
                        JOB_CITY = %s and JOB_AREA = %s and JOB_SALARY = %s and JOB_DATE = %s '''
    nums, result = my_database.select_record(select_sql,
                                             (job_name, job_company, job_city, job_area, job_salary, job_date))
    if nums == 0:
        insert_sql = '''insert into JOB_DETAIL(JOB_NAME,JOB_COMPANY,JOB_CITY,JOB_AREA,JOB_SALARY,JOB_DATE)
                                    VALUES (%s,%s,%s,%s,%s,%s)'''
        my_database.insert_record(insert_sql, (job_name, job_company, job_city, job_area, job_salary, job_date))


# 重组请求数据参数
def get_params(page):
    page = page
    data = {
        "jobarea": "000000,00",
        "district": "000000",
        "funtype": "0000",
        "industrytype": "00",
        "issuedate": 9,
        "providesalary": 99,
        "keywordtype": 2,
        "stype": 1,
        "postchannel": "0000",
        "workyear": "99",
        "cotype": "99",
        "degreefrom": "99",
        "jobterm": "99",
        "companysize": "99",
        "lonlat": "0,0",
        "radius": -1,
        "ord_field": 0,
        "list_type": 0,
        "dibiaoid": 0,
        "confirmdate": 9,
        "curr_page": page
    }
    return data


# 获取数据页数
def get_page_nums(url):
    soup = get_html(url, get_params(1))
    soup = soup.find(attrs={"class": "dw_page"})
    if soup:
        soup = soup.find("span", text=re.compile("^共\d*页"))
        content = soup.text
        result = re.match("^共(\d*)页", content)
        return int(result.group(1))
    else:
        return 0


def main():
    url = "http://search.51job.com/jobsearch/search_result.php?fromJs=1&lang=c"
    page = get_page_nums(url)
    print("共"+str(page)+"页")
    if page:
        for i in range(1, page+1):
            try:
                my_database = MysqlObject(DATABASE_ADDRESS, DATABASE_PORT,
                                          DATABASE_USERNAME, DATABASE_PASSWORD, DATABASE_NAME)
                print("爬取第"+str(i)+"页")
                soup = get_html(url, get_params(i))
                job_list = parse_html(soup)
                for index, job in enumerate(job_list):
                    # save_file(job)
                    save_database(job, my_database)
            except Exception as e:
                print("process error : %s" % e)
                time.sleep(REQUEST_ERROR_SLEEP_TIME)
            finally:
                my_database.close_mysql_conn()
            time.sleep(REQUEST_INTERVAL_SLEEP_TIME)


if __name__ == '__main__':
    main()
