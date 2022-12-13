#!/usr/bin/env python
# coding: utf-8

# In[15]:


from selenium.webdriver import Chrome
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import time
import pandas as pd
import os
import re
import datetime
from sqlalchemy import create_engine
import psycopg2
from prefect import Flow,task
from datetime import datetime as dt
from prefect.schedules import IntervalSchedule


# In[ ]:





# ## The Punch Newspaper

# In[2]:


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def punch_news():
    options = Options()
    options.headless = True
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')

    c = DesiredCapabilities.CHROME
    c["pageLoadStrategy"] = "none"
    #set chromodriver.exe path
    driver = webdriver.Chrome(r'C:\Users\HP\Downloads\news\News_station_analysis\chromedriver.exe',desired_capabilities=c,options=options)
    #explicit wait
    w = WebDriverWait(driver, 20)
    ##
    ##
    ##       EXTRACTION
    ##
    ##
    driver.get("https://punchng.com/topics/news/")
    driver.implicitly_wait(20)
    time.sleep(3)
    #driver.implicitly_wait(20)
    #expected condition
    w.until(EC.presence_of_element_located((By.CLASS_NAME, 'post-title')))
    #JavaScript Executor to stop page load

    driver.execute_script("window.stop();")
    print('First huddle')

    content = []
    contents =driver.find_elements_by_class_name("post-title")
    for con in contents:
        cont = con.get_attribute('innerHTML')
        content.append(cont)
    af = pd.DataFrame(content,columns =['content'])
    af.content = af.content.apply(lambda x: x.replace('<a href=', ''))
    af.content = af.content.apply(lambda x: x.replace('</a>', ''))
    af.content = af.content.apply(lambda x: x.replace('>', '|'))
    af = af['content'].str.split("|",n = 3, expand = True)
    af.columns = ['link','title']
    af = af.drop_duplicates(subset=["link"], keep='first')
    print('Second huddle')
    full_contents = []
    dates = []
    by = []
    def all_news(ev):
        h = WebDriverWait(driver, 20)
        full = []
        timed = []
        print('Pages extraction in progress')

        driver.get(ev)
        time.sleep(4)
        driver.implicitly_wait(20)
        h.until(EC.presence_of_element_located((By.CLASS_NAME, 'post-content')))
        #JavaScript Executor to stop page load
        driver.execute_script("window.stop();")
        full_content = driver.find_elements_by_class_name("post-content")
        for conten in full_content:
            co = conten.get_attribute('innerText')
            co1 = co.replace('\n\n',' ')
            co2 = co1.replace('\n',' ')
            co3 = co2.split(',', 1)
            full.append(co3)

        date = driver.find_elements_by_class_name("col-lg-4")
        for dat in date:
            dat1= dat.get_attribute('innerText')
            dat2 = dat1.replace('By\xa0\n','')
            timed.append(dat2)
        full_contents.append(full[0])
        dates.append(timed[0])
        by.append(timed[1])

    m =af.link.to_list()
    m =  [item.replace('"', '') for item in m]
    for o in m:
        all_news(o)

    driver.quit()
    ##
    ##
    ##       TRANSFORMATION
    ##
    ##
    aa = pd.DataFrame({'Title':af.title,'Full_content': full_contents,'Date':dates,'Author':by,'Source_link':af.link})
    ff  =aa['Full_content'].apply(lambda x: ' '.join(dict.fromkeys(x).keys()))#unlist the full_content column

    aa['Words_count'] = ff.str.split().str.len()#counts the full_content
    n = open("negative-words.txt", "r")
    p = open("positive-words.txt", "r")
    n_word = n.read()
    p_word = p.read()
    n.close()
    p.close()
    n_word=n_word.replace('\n',',')
    n_word = re.sub("[^\w]", " ", n_word).split()
    p_word=p_word.replace('\n',',')
    p_word = re.sub("[^\w]", " ", p_word).split()
    aa['Full_content'] = ff
    #df['word_overlap'] = [set(x[0].split()) & set(x[1].split()) for x in df.values]
    def negative_words(x):
        negative_score = 0
        for word in n_word:
            if word in x:
                negative_score += 1
        return negative_score

    def positive_words(x):
        positive_score = 0
        for word in p_word:
            if word in x:
                positive_score += 1
        return positive_score
    aa['Negative_words'] = aa['Full_content'].apply(lambda x : negative_words(x))
    aa['Positive_words'] = aa['Full_content'].apply(lambda x : positive_words(x))
    
    aa['Sentence_count'] = aa['Full_content'].str.count('[\w][\.!\?]')
    aa['Sentiment'] = round((aa['Positive_words'] - aa['Negative_words']) / aa['Words_count'], 2)
    aa['News_type'] = ['Bad News' if x < 0 else 'Good News' if x > 0 else 'Neutral' for x in aa.Sentiment]
    aa['database_time'] = dt.now()
    ##
    ##
    ##       LOADING
    ##
    ##
    conn_string = 'postgres://testtech@testtech:Useyourpassword1@testtech.postgres.database.azure.com/postgres'
    db = create_engine(conn_string)
    conn = db.connect()
    aa.to_sql('punch_data', con=conn, if_exists='append',
            index=False)
    conn = psycopg2.connect(database='postgres',
                                    user='testtech@testtech', 
                                    password='Useyourpassword1',
                                    host='testtech.postgres.database.azure.com'
            )
    conn.autocommit = True
    cursor = conn.cursor()

    sql1 = '''SELECT COUNT(*) FROM punch_data;'''
    cursor.execute(sql1)
    for i in cursor.fetchall():
        print(i)

    conn.close()
    


# In[ ]:





# ## Vanguard Newspaper

# In[22]:


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def vanguard_news():
    options = Options()
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')
    options.headless = True
    c = DesiredCapabilities.CHROME
    c["pageLoadStrategy"] = "none"
    #set chromodriver.exe path

    driver = webdriver.Chrome(r'C:\Users\HP\Downloads\news\News_station_analysis\chromedriver.exe',desired_capabilities=c,options=options)
    ##
    ##
    ##       EXTRACTION
    ##
    ##

    driver.get("https://www.vanguardngr.com/news/")
    #explicit wait
    w = WebDriverWait(driver, 20)
    #launch URL
    #driver.get("https://www.vanguardngr.com/category/headlines/")
    driver.implicitly_wait(20)
    time.sleep(3)
    #driver.implicitly_wait(20)
    #expected condition
    w.until(EC.presence_of_element_located((By.CLASS_NAME, 'entry-title')))
    #JavaScript Executor to stop page load

    driver.execute_script("window.stop();")
    print('First huddle')

    content = []
    contents =driver.find_elements_by_class_name("entry-title")
    for con in contents:
        cont = con.get_attribute('innerHTML')
        content.append(cont)

    af = pd.DataFrame(content,columns =['content'])
    af.content = af.content.apply(lambda x: x.replace('<a href="', ''))
    af = af.iloc[1:]
    af = af.reset_index()
    af.content = af.content.apply(lambda x: x.replace('rel="bookmark">', ' | '))
    af.content = af.content.apply(lambda x: x.replace('</a>', ' '))
    af = af['content'].str.split("|",n = 3, expand = True)
    af.columns = ['News_link','Title']
    sd = af.head(5)
    driver.quit()
    full_contents = []
    dates = []
    datetime = []
    genres = []
    def all_news(ev):
        options = Options()
        options.headless = True
        ua = UserAgent()
        userAgent = ua.random
        options.page_load_strategy = 'eager'
        options.add_argument(f'user-agent={userAgent}')
        driver = webdriver.Chrome(r'C:\Users\HP\Downloads\news\News_station_analysis\chromedriver.exe',options=options)
        driver.get(ev)
        driver.implicitly_wait(20)
        time.sleep(10)

        full_content = driver.find_elements_by_class_name("entry-content")
        for conten in full_content:
            co = conten.get_attribute('innerText')
            co1 = co.replace('\n\n','')
            co2 = co1.replace('Subscribe for latest Videos','')
            #co3 = co2[co2.find('|'):] #deletes anything before the |
            co3 = co2.replace('\n',' ')
            co4 = co3.split(',',1)
            full_contents.append(co4)

        date = driver.find_elements_by_class_name("entry-date.published.updated")
        for dat in date:
            date= dat.get_attribute('innerText')
            dates.append(date)
            tim= dat.get_attribute('dateTime')
            datetime.append(tim)

        genre = driver.find_elements_by_xpath("""//*[@id="main"]/header/span/a""")
        for gen in genre:
            gen1= gen.get_attribute('innerText')
            #gen2 = gen1.replace('POSTED IN\n','')
            genres.append(gen1)
        print('going')
        driver.quit()
    ##
    ##
    ##       TRANSFORMATION
    ##
    ##

    a = sd.News_link.to_list()
    a =  [item.replace('"  ', '') for item in a]
    #a = ['https://www.vanguardngr.com/2022/12/mavins-marks-10th-anniversary-with-new-album/']
    for i in a:
        all_news(i)

    ss = pd.DataFrame({'Full_content': full_contents,'Date':dates,'Time_published':datetime})
    ff  =ss['Full_content'].apply(lambda x: ' '.join(dict.fromkeys(x).keys()))

    ss['Words_count'] = ff.str.split().str.len()
    n = open("negative-words.txt", "r")
    p = open("positive-words.txt", "r")
    n_word = n.read()
    p_word = p.read()
    n.close()
    p.close()
    n_word=n_word.replace('\n',',')
    n_word = re.sub("[^\w]", " ", n_word).split()
    p_word=p_word.replace('\n',',')
    p_word = re.sub("[^\w]", " ", p_word).split()
    #df['word_overlap'] = [set(x[0].split()) & set(x[1].split()) for x in df.values]
    def negative_words(x):
        negative_score = 0
        for word in n_word:
            if word in x:
                negative_score += 1
        return negative_score

    def positive_words(x):
        positive_score = 0
        for word in p_word:
            if word in x:
                positive_score += 1
        return positive_score
    ss['Full_content'] = ff
    ss['database_time'] = dt.now()
    ss['Negative_words'] = ss['Full_content'].apply(lambda x : negative_words(x))
    ss['Positive_words'] = ss['Full_content'].apply(lambda x : positive_words(x))
    ss['Sentence_count'] = ss['Full_content'].str.count('[\w][\.!\?]')
    #ss['database_time'] = datetime.datetime.now()

    ss['Sentiment'] = round((ss['Positive_words'] - ss['Negative_words']) / ss['Words_count'], 2)
    ss['News_type'] = ['Bad News' if x < 0 else 'Good News' if x > 0 else 'Neutral' for x in ss.Sentiment]
    va = pd.concat([sd,ss], axis=1)
    print(va)
    ##
    ##
    ##       LOADING
    ##
    ##
    conn_string = 'postgres://testtech@testtech:Useyourpassword1@testtech.postgres.database.azure.com/postgres'
    db = create_engine(conn_string)
    conn = db.connect()

    va.to_sql('vanguard_data', con=conn, if_exists='append',
            index=False)
    conn = psycopg2.connect(database='postgres',
                                    user='testtech@testtech', 
                                    password='Useyourpassword1',
                                    host='testtech.postgres.database.azure.com'
            )
    conn.autocommit = True
    cursor = conn.cursor()

    sql1 = '''SELECT COUNT(*) FROM vanguard_data;'''
    cursor.execute(sql1)
    for i in cursor.fetchall():
        print(i)

    conn.close()


# In[ ]:





# ## The Nation Newspaper

# In[102]:


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def the_nation():
    options = Options()
    options.headless = True
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')

    c = DesiredCapabilities.CHROME
    c["pageLoadStrategy"] = "none"
    #set chromodriver.exe path
    driver = webdriver.Chrome(r'C:\Users\HP\Downloads\news\News_station_analysis\chromedriver.exe',desired_capabilities=c,options=options)
    #explicit wait
    w = WebDriverWait(driver, 20)
    ##
    ##
    ##       EXTRACTION
    ##
    ##
    driver.get("https://thenationonlineng.net/news/")
    driver.implicitly_wait(20)

    #driver.implicitly_wait(20)
    #expected condition
    w.until(EC.presence_of_element_located((By.CLASS_NAME, 'nation_category_post_title')))
    #JavaScript Executor to stop page load

    driver.execute_script("window.stop();")
    print('First huddle')

    content = []
    contents =driver.find_elements_by_css_selector('section section a')
    for con in contents:
        cont = con.get_attribute('href')
        content.append(cont)

    af = pd.DataFrame(content,columns =['content'])
    af = af.drop_duplicates(subset=["content"], keep='first')
    #af = af[af["content"].str.contains("https://thenationonlineng.net/news/page/2/") == False]
    discard = ["https://thenationonlineng.net/news/page/"]

    # drop rows that contain the partial string "Sci"
    af = af[~af.content.str.contains('|'.join(discard))]
    titless = []
    by = []
    date = []
    full_content = []

    def all_news(ev):
        h = WebDriverWait(driver, 20)
        driver.get(ev)
        time.sleep(5)
        driver.implicitly_wait(20)
        h.until(EC.presence_of_element_located((By.CLASS_NAME, 'nation__article__content')))
        #JavaScript Executor to stop page load
        driver.execute_script("window.stop();")
        title = driver.find_elements_by_css_selector('section article header h1')
        for tit in title:
            titl = tit.get_attribute('innerText')
            titless.append(titl)

        who = driver.find_elements_by_class_name("nation__article__meta")
        for wh in who:
            w = wh.get_attribute('outerText')
            w = w.replace('\n',' | ')
            w = w.split('|',1)

            by.append(w[0])
            date.append(w[1])
        print('Pages extraction in progress')
        article = driver.find_elements_by_class_name('nation__article__content')
        for art in article:
            arti = art.get_attribute('innerText')
            arti = arti.replace('\n\n','')
            arti = arti.split('ADVERTISEMENTS')[0]
            full_content.append(arti)

    for i in af.content:
        all_news(i)
    driver.quit() 
    ##
    ##
    ##       TRANSFORMATION
    ##
    ##
    ss = pd.DataFrame({'Title': titless,'Full_content': full_content,'Date':date,'Author':by,'News_link':af.content})
    ss['Words_count'] = ss.Full_content.str.split().str.len()
    n = open("negative-words.txt", "r")
    p = open("positive-words.txt", "r")
    n_word = n.read()
    p_word = p.read()
    n.close()
    p.close()
    n_word=n_word.replace('\n',',')
    n_word = re.sub("[^\w]", " ", n_word).split()
    p_word=p_word.replace('\n',',')
    p_word = re.sub("[^\w]", " ", p_word).split()
    def negative_words(x):
        negative_score = 0
        for word in n_word:
            if word in x:
                negative_score += 1
        return negative_score

    def positive_words(x):
        positive_score = 0
        for word in p_word:
            if word in x:
                positive_score += 1
        return positive_score

    ss['Negative_words'] = ss['Full_content'].apply(lambda x : negative_words(x))
    ss['Positive_words'] = ss['Full_content'].apply(lambda x : positive_words(x))
    ss['Sentence_count'] = ss['Full_content'].str.count('[\w][\.!\?]')

    ss['Sentiment'] = round((ss['Positive_words'] - ss['Negative_words']) / ss['Words_count'], 2)
    ss['News_type'] = ['Bad News' if x < 0 else 'Good News' if x > 0 else 'Neutral' for x in ss.Sentiment]
    ss['database_time'] = dt.now()
    ##
    ##
    ##       LOADING
    ##
    ##
    conn_string = 'postgres://testtech@testtech:Useyourpassword1@testtech.postgres.database.azure.com/postgres'
    db = create_engine(conn_string)
    conn = db.connect()

    ss.to_sql('nation_data', con=conn, if_exists='append',
            index=False)
    conn = psycopg2.connect(database='postgres',
                                    user='testtech@testtech', 
                                    password='Useyourpassword1',
                                    host='testtech.postgres.database.azure.com'
            )
    conn.autocommit = True
    cursor = conn.cursor()

    sql1 = '''SELECT COUNT(*) FROM nation_data;'''
    cursor.execute(sql1)
    for i in cursor.fetchall():
        print(i)

    conn.close()


# ## The Guardian Nigeria

# In[45]:


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def the_guardian():
    options = Options()
    options.headless = True
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')

    c = DesiredCapabilities.CHROME
    c["pageLoadStrategy"] = "none"
    #set chromodriver.exe path
    driver = webdriver.Chrome(r'C:\Users\HP\Downloads\news\News_station_analysis\chromedriver.exe',desired_capabilities=c,options=options)
    #explicit wait
    w = WebDriverWait(driver, 20)
    ##
    ##
    ##       EXTRACTION
    ##
    ##
    driver.get("https://guardian.ng/latest/")
    driver.implicitly_wait(20)

    #driver.implicitly_wait(20)
    #expected condition
    w.until(EC.presence_of_element_located((By.CLASS_NAME, 'title')))
    #JavaScript Executor to stop page load

    driver.execute_script("window.stop();")
    print('First huddle')

    content = []
    contents =driver.find_elements_by_css_selector('div main section div div div span a')
    for con in contents:
        cont = con.get_attribute('href')
        content.append(cont)
    af = pd.DataFrame(content,columns =['content'])   
    titless = []
    by = []
    dates = []
    full_content = []

    def all_news(ev):
        fully = []
        #driver = webdriver.Chrome(r'C:\Users\HP\Downloads\news\News_station_analysis\chromedriver.exe',desired_capabilities=c,options=options)
        h = WebDriverWait(driver, 20)
        driver.get(ev)
        time.sleep(5)
        driver.implicitly_wait(20)
        h.until(EC.presence_of_element_located((By.CLASS_NAME, 'content')))
        #JavaScript Executor to stop page load
        driver.execute_script("window.stop();")
        title = driver.find_elements_by_css_selector('div main div h1')
        for tit in title:
            titl = tit.get_attribute('innerText')
            titless.append(titl)
        print('Extration in progress')
        bywho = driver.find_elements_by_css_selector('div main div div div div strong')
        for byw in bywho:
            who = byw.get_attribute('innerText')
            by.append(who)

        date = driver.find_elements_by_class_name('date')
        for dat in date:
            d = dat.get_attribute('innerText')
            d = d.replace('\xa0 | \xa0', 'at')
            dates.append(d)
        #driver.find_elements_by_css_selector('div main div div div p')   
        full_cont = driver.find_elements_by_css_selector('div main div div div p')  
        for full in full_cont:
            full_c = full.get_attribute('innerText')
            full_c = full_c.replace('\nDownload logo\n', '')
            full_c = full_c.replace('\xa0', '')
            #full_c = [full_c]
            #full_c = ' '.join(full_c)
            fully.append(full_c)

        article = ' '.join(fully)
        full_content.append(article)

    for i in af.content:
        all_news(i)

    driver.quit()
    ##
    ##
    ##       TRANSFORMATION
    ##
    ##
    ss = pd.DataFrame({'Title': titless,'Full_content': full_content,'Date':dates,'Author':by,'News_link':af.content})
    ss['Words_count'] = ss.Full_content.str.split().str.len()
    n = open("negative-words.txt", "r")
    p = open("positive-words.txt", "r")
    n_word = n.read()
    p_word = p.read()
    n.close()
    p.close()
    n_word=n_word.replace('\n',',')
    n_word = re.sub("[^\w]", " ", n_word).split()
    p_word=p_word.replace('\n',',')
    p_word = re.sub("[^\w]", " ", p_word).split()
    def negative_words(x):
        negative_score = 0
        for word in n_word:
            if word in x:
                negative_score += 1
        return negative_score

    def positive_words(x):
        positive_score = 0
        for word in p_word:
            if word in x:
                positive_score += 1
        return positive_score

    ss['Negative_words'] = ss['Full_content'].apply(lambda x : negative_words(x))
    ss['Positive_words'] = ss['Full_content'].apply(lambda x : positive_words(x))
    ss['Sentence_count'] = ss['Full_content'].str.count('[\w][\.!\?]')

    ss['Sentiment'] = round((ss['Positive_words'] - ss['Negative_words']) / ss['Words_count'], 2)
    ss['News_type'] = ['Bad News' if x < 0 else 'Good News' if x > 0 else 'Neutral' for x in ss.Sentiment]
    ss['database_time'] = dt.now()
    ##
    ##
    ##       LOADING
    ##
    ##
    conn_string = 'postgres://testtech@testtech:Useyourpassword1@testtech.postgres.database.azure.com/postgres'
    db = create_engine(conn_string)
    conn = db.connect()

    ss.to_sql('guardian_data', con=conn, if_exists='append',
            index=False)
    conn = psycopg2.connect(database='postgres',
                                    user='testtech@testtech', 
                                    password='Useyourpassword1',
                                    host='testtech.postgres.database.azure.com'
            )
    conn.autocommit = True
    cursor = conn.cursor()

    sql1 = '''SELECT COUNT(*) FROM guardian_data;'''
    cursor.execute(sql1)
    for i in cursor.fetchall():
        print(i)

    conn.close()


# In[ ]:





# ## The Sun Nigeria

# In[25]:


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def the_sun():
    options = Options()
    options.headless = True
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')

    c = DesiredCapabilities.CHROME
    c["pageLoadStrategy"] = "none"
    #set chromodriver.exe path
    driver = webdriver.Chrome(r'C:\Users\HP\Downloads\news\News_station_analysis\chromedriver.exe',desired_capabilities=c,options=options)
    #explicit wait
    w = WebDriverWait(driver, 20)
    ##
    ##
    ##       EXTRACTION
    ##
    ##
    driver.get("https://www.sunnewsonline.com/category/news/")
    driver.implicitly_wait(20)

    #driver.implicitly_wait(20)
    #expected condition
    w.until(EC.presence_of_element_located((By.CLASS_NAME, 'jeg_post_title')))
    #JavaScript Executor to stop page load

    driver.execute_script("window.stop();")
    print('First huddle')

    content = []
    contents =driver.find_elements_by_css_selector('div div article div h3 a')
    for con in contents:
        cont = con.get_attribute('href')
        content.append(cont)
    af = pd.DataFrame(content,columns =['content']) 
    driver.quit()
    titless = []
    dates = []
    full_content = []

    def all_news(ev):
        options = Options()
        options.headless = True
        ua = UserAgent()
        userAgent = ua.random
        options.page_load_strategy = 'eager'
        options.add_argument(f'user-agent={userAgent}')
        driver = webdriver.Chrome(r'C:\Users\HP\Downloads\news\News_station_analysis\chromedriver.exe',options=options)
        driver.get(ev)
        driver.implicitly_wait(20)
        time.sleep(10)


        title = driver.find_elements_by_css_selector('div div div div div div h1')
        for tit in title:
            titl = tit.get_attribute('innerText')
            titless.append(titl)

        print('Extration in progress')
        date = driver.find_elements_by_class_name('jeg_meta_date')
        for dat in date:
            d = dat.get_attribute('innerText')
            dates.append(d)

        full_cont = driver.find_elements_by_class_name('content-inner')
        for full in full_cont:
            full_c = full.get_attribute('innerText')
            full_c = full_c.replace('\n',' ') 
            full_content.append(full_c)
        driver.quit()

    for i in af.content:
        all_news(i)
    ##
    ##
    ##       TRANSFORMATION
    ##
    ##
    ss = pd.DataFrame({'Title': titless,'Full_content': full_content,'News_link':af.content})
    ss['Words_count'] = ss.Full_content.str.split().str.len()
    n = open("negative-words.txt", "r")
    p = open("positive-words.txt", "r")
    n_word = n.read()
    p_word = p.read()
    n.close()
    p.close()
    n_word=n_word.replace('\n',',')
    n_word = re.sub("[^\w]", " ", n_word).split()
    p_word=p_word.replace('\n',',')
    p_word = re.sub("[^\w]", " ", p_word).split()
    def negative_words(x):
        negative_score = 0
        for word in n_word:
            if word in x:
                negative_score += 1
        return negative_score

    def positive_words(x):
        positive_score = 0
        for word in p_word:
            if word in x:
                positive_score += 1
        return positive_score

    ss['Negative_words'] = ss['Full_content'].apply(lambda x : negative_words(x))
    ss['Positive_words'] = ss['Full_content'].apply(lambda x : positive_words(x))
    ss['Sentence_count'] = ss['Full_content'].str.count('[\w][\.!\?]')

    ss['Sentiment'] = round((ss['Positive_words'] - ss['Negative_words']) / ss['Words_count'], 2)
    ss['News_type'] = ['Bad News' if x < 0 else 'Good News' if x > 0 else 'Neutral' for x in ss.Sentiment]
    ss['database_time'] = dt.now()
    ##
    ##
    ##       LOADING
    ##
    ##
    #conn_string = dbname='postgres' user='testtech@testtech' host='testtech.postgres.database.azure.com' password='Useyourpassword1' port='5432' sslmode='true'
    conn_string = 'postgres://testtech@testtech:Useyourpassword1@testtech.postgres.database.azure.com/postgres'
    db = create_engine(conn_string)
    conn = db.connect()

    ss.to_sql('sun_data', con=conn, if_exists='append',
            index=False)
    conn = psycopg2.connect(database='postgres',
                                    user='testtech@testtech', 
                                    password='Useyourpassword1',
                                    host='testtech.postgres.database.azure.com'
            )
    conn.autocommit = True
    cursor = conn.cursor()

    sql1 = '''SELECT COUNT(*) FROM sun_data;'''
    cursor.execute(sql1)
    for i in cursor.fetchall():
        print(i)

    conn.close()
   


# ## Automation and Scheduling with PREFECT
# #### Runs every 3 hours

# In[1]:


def flow_caso(schedule=None):
    with Flow("news_pipeline",schedule=schedule) as flow:
        
        Punch_news = punch_news()
        Vanguard_news = vanguard_news()
        The_nation_news = the_nation()
        The_guardian = the_guardian()
        The_Sun = the_sun()        

    return flow


schedule = IntervalSchedule(
    start_date = datetime.datetime.now() + datetime.timedelta(seconds = 2),
    interval = datetime.timedelta(hours=3)
)
flow=flow_caso(schedule)

flow.run()


# In[ ]:





# In[ ]:




