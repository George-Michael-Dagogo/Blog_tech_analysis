from selenium.webdriver import Chrome
#for connecting to site with Chrome
from selenium.webdriver.support.ui import WebDriverWait
#for waiting until an element is identified on the site before running a command
from selenium.webdriver.common.by import By
#for selecting an element 'By"
from selenium import webdriver
#for connecting the driver to Chrome
from selenium.webdriver.chrome.options import Options
#for for manipulating certain attributes of the Chromedriver
from selenium.webdriver.support import expected_conditions as EC
#for an expected condition to be met before running next line of code
from fake_useragent import UserAgent
#for giving a different header name each time a request is made
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
#for adding capabilities to your driver
import time
#code sleep time
import pandas as pd
#majority of the transformation was done with pandas
import os
#for accessing and manipulating the os in use
import re
#for data transformation
from sqlalchemy import create_engine
#for connecting and pushing data to the database
import psycopg2
#for getting data from the database
from prefect import Flow,task
#for arranging the flow of the pipeline
from datetime import datetime as dt
#using dates
from prefect.schedules import IntervalSchedule
#for scheduling the flow through functions
from email.message import EmailMessage
#for email notification
import ssl
#for or keeping an internet connection secure and safeguarding any sensitive data
import smtplib
#for sending the mails
import numpy as np
#for data transformation
import dataframe_image as dfi
#for saving a dataframe as a picture
from nltk.corpus import stopwords
#for sentiment analysis
import matplotlib.pyplot as plt
#for visualization
from os import listdir
#for getting the directory via os lib


# In[ ]:





# ## The Punch Newspaper

# In[2]:


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def punch_news():
    options = Options()
    options.headless = True
    #running the function without opening a chrome tab
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')
    #implementing the fake_user_agent lib
    c = DesiredCapabilities.CHROME
    c["pageLoadStrategy"] = "none"
    #set chromodriver.exe path
    driver = webdriver.Chrome(r'pathto\chromedriver.exe',desired_capabilities=c,options=options)
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
    #expected condition
    w.until(EC.presence_of_element_located((By.CLASS_NAME, 'post-title')))
    driver.execute_script("window.stop();")
    #JavaScript Executor to stop page load
    print('First huddle')

    content = []
    #a list that holds the news link and title
    contents =driver.find_elements_by_class_name("post-title")
    for con in contents:
        cont = con.get_attribute('innerHTML')
        content.append(cont)
    af = pd.DataFrame(content,columns =['content'])
    af.content = af.content.apply(lambda x: x.replace('<a href=', ''))
    af.content = af.content.apply(lambda x: x.replace('</a>', ''))
    af.content = af.content.apply(lambda x: x.replace('>', '|'))
    #removes unwanted part of what selenium returned
    af = af['content'].str.split("|",n = 3, expand = True)
    af.columns = ['link','title']
    af = af.drop_duplicates(subset=["link"], keep='first')
    #removes duplicates
    print('Second huddle')
    full_contents = []
    #this list stores the full detailed news
    dates = []
    #stores the news dates
    by = []
    #this list stores the author's name 
    def all_news(ev):
        ''' this function has an argument which takes the news link, gets the detailed news, date and author
        '''
        h = WebDriverWait(driver, 20)
        full = []
        timed = []
        print('Pages extraction in progress')

        driver.get(ev)
        time.sleep(4)
        driver.implicitly_wait(20)
        h.until(EC.presence_of_element_located((By.CLASS_NAME, 'post-content')))
        #waits until a class name is identified
        driver.execute_script("window.stop();")
        #JavaScript Executor to stop page load
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
    #changes the pandas dataframe to a list
    m =  [item.replace('"', '') for item in m]
    for o in m:
        all_news(o)
    #for loop that goes through every link in the af.link column and returns required data

    driver.quit()
    #stops the driver
    ##
    ##
    ##       TRANSFORMATION
    ##
    ##
    aa = pd.DataFrame({'Title':af.title,'Full_content': full_contents,'Date':dates,'Author':by,'Source_link':af.link})
    #changes the webcraped data into a pandas dataframe
    ff  =aa['Full_content'].apply(lambda x: ' '.join(dict.fromkeys(x).keys()))
    #unlist the full_content column

    aa['Words_count'] = ff.str.split().str.len()
    #counts the words in the full_content column
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
    
    def negative_words(x):
        '''this function checks if any word in the negative words text file matches any word in the full_content column
        and adds 1 to the negative_score variable if a match is spotted
        this is also a pandas UDF
        '''
        negative_score = 0
        for word in n_word:
            if word in x:
                negative_score += 1
        return negative_score

    def positive_words(x):
        '''this function checks if any word in the positive words text file matches any word in the full_content column
        and adds 1 to the positive _score variable if a match is spotted
        this is also a pandas UDF
        '''
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
    #this is needed to delete any row in the database that is longer than a particular time
    ##
    ##
    ##       LOADING
    ##
    ##
    conn_string = 'postgres://testtech@testtech:Useyourpassword1@testtech.postgres.database.azure.com/postgres'
    #conn_string = potgress://user:password@host/database
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

    #Deletes any rows older than 2 days
    sql1 = '''DELETE FROM punch_data WHERE database_time < current_timestamp - interval '2' day;'''
    cursor.execute(sql1)
    
    #Removes duplicates from the database table based on the Title column
    sql2 = '''DELETE FROM punch_data T1 USING punch_data T2 WHERE T1.ctid < T2.ctid AND  'T1.Title' = 'T2.Title';'''
    cursor.execute(sql2)

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

    driver = webdriver.Chrome(r'pathto\chromedriver.exe',desired_capabilities=c,options=options)
    ##
    ##
    ##       EXTRACTION
    ##
    ##
    driver.get("https://www.vanguardngr.com/news/")
    #explicit wait
    w = WebDriverWait(driver, 20)
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
    #the first row had unwanted elements 
    af = af.reset_index()
    af.content = af.content.apply(lambda x: x.replace('rel="bookmark">', ' | '))
    af.content = af.content.apply(lambda x: x.replace('</a>', ' '))
    af = af['content'].str.split("|",n = 3, expand = True)
    af.columns = ['News_link','Title']
    sd = af.head(10)
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
        #this option allows only the html page to load and stops the loading for extraction  to continue
        options.add_argument(f'user-agent={userAgent}')
        driver = webdriver.Chrome(r'pathto\chromedriver.exe',options=options)
        driver.get(ev)
        driver.implicitly_wait(20)
        time.sleep(10)

        full_content = driver.find_elements_by_class_name("entry-content")
        for conten in full_content:
            co = conten.get_attribute('innerText')
            co1 = co.replace('\n\n','')
            co2 = co1.replace('Subscribe for latest Videos','')
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
    ss['Sentiment'] = round((ss['Positive_words'] - ss['Negative_words']) / ss['Words_count'], 2)
    ss['News_type'] = ['Bad News' if x < 0 else 'Good News' if x > 0 else 'Neutral' for x in ss.Sentiment]
    va = pd.concat([sd,ss], axis=1)
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

    #Deletes any rows older than 2 days
    sql1 = '''DELETE FROM vanguard_data WHERE database_time < current_timestamp - interval '2' day;'''
    cursor.execute(sql1)
    
    #Removes duplicates from the database table based on the Title column
    sql2 = '''DELETE FROM vanguard_data T1 USING vanguard_data T2 WHERE T1.ctid < T2.ctid AND  'T1.Title' = 'T2.Title';'''
    cursor.execute(sql2)

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
    driver = webdriver.Chrome(r'pathto\chromedriver.exe',desired_capabilities=c,options=options)
    #explicit wait
    w = WebDriverWait(driver, 20)
    ##
    ##
    ##       EXTRACTION
    ##
    ##
    driver.get("https://thenationonlineng.net/news/")
    driver.implicitly_wait(20)
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
    discard = ["https://thenationonlineng.net/news/page/"]
    # drop rows that contain the discard string 
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

    #Deletes any rows older than 2 days
    sql1 = '''DELETE FROM nation_data WHERE database_time < current_timestamp - interval '2' day;'''
    cursor.execute(sql1)
    
    #Removes duplicates from the database table based on the Title column
    sql2 = '''DELETE FROM nation_data T1 USING nation_data T2 WHERE T1.ctid < T2.ctid AND  'T1.Title' = 'T2.Title';'''
    cursor.execute(sql2)

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
    driver = webdriver.Chrome(r'pathto\chromedriver.exe',desired_capabilities=c,options=options)
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
        full_cont = driver.find_elements_by_css_selector('div main div div div p')  
        for full in full_cont:
            full_c = full.get_attribute('innerText')
            full_c = full_c.replace('\nDownload logo\n', '')
            full_c = full_c.replace('\xa0', '')
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

    #Deletes any rows older than 2 days
    sql1 = '''DELETE FROM guardian_data WHERE database_time < current_timestamp - interval '2' day;'''
    cursor.execute(sql1)
    
    #Removes duplicates from the database table based on the Title column
    sql2 = '''DELETE FROM guardian_data T1 USING guardian_data T2 WHERE T1.ctid < T2.ctid AND  'T1.Title' = 'T2.Title';'''
    cursor.execute(sql2)

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
    driver = webdriver.Chrome(r'pathto\chromedriver.exe',desired_capabilities=c,options=options)
    #explicit wait
    w = WebDriverWait(driver, 20)
    ##
    ##
    ##       EXTRACTION
    ##
    ##
    driver.get("https://www.sunnewsonline.com/category/news/")
    driver.implicitly_wait(20)

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
        driver = webdriver.Chrome(r'pathto\chromedriver.exe',options=options)
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

    #Deletes any rows older than 2 days
    sql1 = '''DELETE FROM sun_data WHERE database_time < current_timestamp - interval '2' day;'''
    cursor.execute(sql1)
    
    #Removes duplicates from the database table based on the Title column
    sql2 = '''DELETE FROM sun_data T1 USING sun_data T2 WHERE T1.ctid < T2.ctid AND  'T1.Title' = 'T2.Title';'''
    cursor.execute(sql2)

    conn.close()
   


# In[ ]:


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def database_interaction():


    def punch_analytics():
        conn = psycopg2.connect(database='postgres',
                                    user='testtech@testtech', 
                                    password='Useyourpassword1',
                                    host='testtech.postgres.database.azure.com'
            )
        conn.autocommit = True
        cursor = conn.cursor()
        a = []
        b = []
        c = []
        e = []
        
        sql1 = '''SELECT "Title", "Source_link" FROM punch_data WHERE "News_type" = 'Good News' LIMIT 5;'''
        #returns title and news link that has news type as 'Good News'
        cursor.execute(sql1)
        for top_5_good in cursor.fetchall():
            e.append(top_5_good)
        e.append(('there are no good news','if nothing is displayed else '))
        #incase if sql1 returns empty

        sql2 = '''SELECT "News_type", count(*) FROM punch_data WHERE "News_type" is not null GROUP BY "News_type";'''
        #returns the news type and a sum of each news sentiment 
        cursor.execute(sql2)
        for news_type_punch in cursor.fetchall():
            a.append(news_type_punch)

        sql3 = '''SELECT "Author", COUNT("Author") AS author_occurrence 
                    FROM punch_data GROUP BY "Author" ORDER BY 
                    author_occurrence DESC LIMIT 5;'''
        #returns the author that occurs the most and the count
        cursor.execute(sql3)
        for author_top_3_punch in cursor.fetchall():
            c.append(author_top_3_punch)

        sql4 = '''SELECT word, count(*) AS ct FROM punch_data, 
                    unnest(string_to_array("Full_content", ' ')) 
                    word GROUP  BY 1 ORDER  BY 2 DESC LIMIT  100;'''
        #returns the most frequent words without stopwords
        cursor.execute(sql4)
        for most_word in cursor.fetchall():
            b.append(most_word)    


        conn.close()

        news_t = pd.DataFrame(a)
        news_t.columns = ['type','count']
        news_t = news_t.plot(x = 'type',y="count",color=['red','green','grey'],kind="barh",title="Sentiment of The Punch Newspaper",figsize=(10,7))
        news_t.figure.savefig('sentiment_punch.png')
        #plots the sentiment of a news outlet

        authors_t = pd.DataFrame(c)
        authors_t.columns = ['author','count']
        authors_t = authors_t.set_index('author')
        author_st = authors_t.style.background_gradient()
        dfi.export(author_st,"authors_punch.png")
        #saves the most frequent authors as png file


        good_news = pd.DataFrame(e)
        good_news.columns = ['title','link']
        good_news_punch = good_news.set_index('title')

        most_words = pd.DataFrame(b)
        most_words.columns = ['word','count']
        stops =  list(stopwords.words('english'))
        most_words = most_words[~most_words['word'].isin(stops)]
        most_words = most_words[~most_words['word'].str.isnumeric()]
        most_words = most_words.tail(20)
        most_words = most_words.plot(x = 'word',y="count",kind="barh",title="Most Frequent Words in the Punch",figsize=(10,7))
        most_words.figure.savefig('most_words_punch.png')
        return good_news_punch


    def guardian_analytics():
        conn = psycopg2.connect(database='postgres',
                                    user='testtech@testtech', 
                                    password='Useyourpassword1',
                                    host='testtech.postgres.database.azure.com'
            )
        conn.autocommit = True
        cursor = conn.cursor()
        a = []
        b = []
        c = []
        e = []
        sql1 = '''SELECT "Title", "News_link" FROM guardian_data WHERE "News_type" = 'Good News' LIMIT 5;'''
        cursor.execute(sql1)
        for top_5_good in cursor.fetchall():
            e.append(top_5_good)
        e.append(('there are no good news','if nothing is displayed else '))

        sql2 = '''SELECT "News_type", count(*) FROM guardian_data WHERE "News_type" is not null GROUP BY "News_type";'''
        cursor.execute(sql2)
        for news_type in cursor.fetchall():
            a.append(news_type)

        sql3 = '''SELECT "Author", COUNT("Author") AS author_occurrence 
                    FROM guardian_data GROUP BY "Author" ORDER BY 
                    author_occurrence DESC LIMIT 5;'''
        cursor.execute(sql3)
        for author_top in cursor.fetchall():
            c.append(author_top)

        sql4 = '''SELECT word, count(*) AS ct FROM guardian_data, 
                    unnest(string_to_array("Full_content", ' ')) 
                    word GROUP  BY 1 ORDER  BY 2 DESC LIMIT  100;'''
        cursor.execute(sql4)
        for most_word in cursor.fetchall():
            b.append(most_word)    
        conn.close()

        news_t = pd.DataFrame(a)
        news_t.columns = ['type','count']
        news_t = news_t.plot(x = 'type',y="count",color=['red','green','grey'],kind="barh",title="Sentiment of The Guardian Newspaper",figsize=(10,7))
        news_t.figure.savefig('sentiment_guardian.png')

        authors_t = pd.DataFrame(c)
        authors_t.columns = ['author','count']
        authors_t = authors_t.set_index('author')
        author_st = authors_t.style.background_gradient()
        dfi.export(author_st,"authors_guardian.png")


        good_news = pd.DataFrame(e)
        good_news.columns = ['title','link']
        good_news_guardian = good_news.set_index('title')

        most_words = pd.DataFrame(b)
        most_words.columns = ['word','count']
        stops =  list(stopwords.words('english'))
        most_words = most_words[~most_words['word'].isin(stops)]
        most_words = most_words[~most_words['word'].str.isnumeric()]
        most_words = most_words.tail(20)
        #most_words = most_words.set_index('word')
        most_words = most_words.plot(x = 'word',y="count",kind="barh",title="Most Frequent Words in the Guardian",figsize=(10,7))
        most_words.figure.savefig('most_words_guardian.png')
        return good_news_guardian


    def vanguard_analytics():
        conn = psycopg2.connect(database='postgres',
                                    user='testtech@testtech', 
                                    password='Useyourpassword1',
                                    host='testtech.postgres.database.azure.com'
            )
        conn.autocommit = True
        cursor = conn.cursor()
        a = []
        b = []
        e = []
        sql1 = '''SELECT "Title", "News_link" FROM vanguard_data WHERE "News_type" = 'Good News' LIMIT 5;'''
        cursor.execute(sql1)
        for top_5_good in cursor.fetchall():
            e.append(top_5_good)
        e.append(('there are no good news','if nothing is displayed else '))

        sql2 = '''SELECT "News_type", count(*) FROM vanguard_data WHERE "News_type" is not null GROUP BY "News_type";'''
        cursor.execute(sql2)
        for news_type in cursor.fetchall():
            a.append(news_type)

        sql4 = '''SELECT word, count(*) AS ct FROM vanguard_data, 
                    unnest(string_to_array("Full_content", ' ')) 
                    word GROUP  BY 1 ORDER  BY 2 DESC LIMIT  100;'''
        cursor.execute(sql4)
        for most_word in cursor.fetchall():
            b.append(most_word)    



        conn.close()

        news_t = pd.DataFrame(a)
        news_t.columns = ['type','count']
        news_t = news_t.plot(x = 'type',y="count",color=['red','green','grey'],kind="barh",title="Sentiment of The Vanguard Newspaper",figsize=(10,7))
        news_t.figure.savefig('sentiment_vanguard.png')


        good_news = pd.DataFrame(e)
        good_news.columns = ['title','link']
        good_news_vanguard = good_news.set_index('title')

        most_words = pd.DataFrame(b)
        most_words.columns = ['word','count']
        stops =  list(stopwords.words('english'))
        most_words = most_words[~most_words['word'].isin(stops)]
        most_words = most_words[~most_words['word'].str.isnumeric()]
        most_words = most_words.tail(20)
        most_words = most_words.plot(x = 'word',y="count",kind="barh",title="Most Frequent Words in the Vanguard",figsize=(10,7))
        most_words.figure.savefig('most_words_vanguard.png')
        return good_news_vanguard

    def nation_analytics():
        conn = psycopg2.connect(database='postgres',
                                    user='testtech@testtech', 
                                    password='Useyourpassword1',
                                    host='testtech.postgres.database.azure.com'
            )
        conn.autocommit = True
        cursor = conn.cursor()
        a = []
        b = []
        c = []
        e = []
        #punch_data
        sql1 = '''SELECT "Title", "News_link" FROM nation_data WHERE "News_type" = 'Good News' LIMIT 5;'''
        cursor.execute(sql1)
        for top_5_good in cursor.fetchall():
            e.append(top_5_good)
        e.append(('there are no good news','if nothing is displayed else '))

        sql2 = '''SELECT "News_type", count(*) FROM nation_data WHERE "News_type" is not null GROUP BY "News_type";'''
        cursor.execute(sql2)
        for news_type in cursor.fetchall():
            a.append(news_type)

        sql3 = '''SELECT "Author", COUNT("Author") AS author_occurrence 
                    FROM nation_data GROUP BY "Author" ORDER BY 
                    author_occurrence DESC LIMIT 5;'''
        cursor.execute(sql3)
        for author_top in cursor.fetchall():
            c.append(author_top)

        sql4 = '''SELECT word, count(*) AS ct FROM nation_data, 
                    unnest(string_to_array("Full_content", ' ')) 
                    word GROUP  BY 1 ORDER  BY 2 DESC LIMIT  100;'''
        cursor.execute(sql4)
        for most_word in cursor.fetchall():
            b.append(most_word)    



        conn.close()

        news_t = pd.DataFrame(a)
        news_t.columns = ['type','count']
        news_t = news_t.plot(x = 'type',y="count",color=['red','green','grey'],kind="barh",title="Sentiment of The Nation Newspaper",figsize=(10,7))
        news_t.figure.savefig('sentiment_nation.png')

        authors_t = pd.DataFrame(c)
        authors_t.columns = ['author','count']
        authors_t = authors_t.set_index('author')
        author_st = authors_t.style.background_gradient()
        dfi.export(author_st,"authors_nation.png")


        good_news = pd.DataFrame(e)
        good_news.columns = ['title','link']
        good_news_nation = good_news.set_index('title')

        most_words = pd.DataFrame(b)
        most_words.columns = ['word','count']
        stops =  list(stopwords.words('english'))
        most_words = most_words[~most_words['word'].isin(stops)]
        most_words = most_words[~most_words['word'].str.isnumeric()]
        most_words = most_words.tail(20)
        most_words = most_words.plot(x = 'word',y="count",kind="barh",title="Most Frequent Words in the Nation",figsize=(10,7))
        most_words.figure.savefig('most_words_nation.png')
        return good_news_nation

    def sun_analytics():
        conn = psycopg2.connect(database='postgres',
                                    user='testtech@testtech', 
                                    password='Useyourpassword1',
                                    host='testtech.postgres.database.azure.com'
            )
        conn.autocommit = True
        cursor = conn.cursor()
        a = []
        b = []
        e = []
        
        sql1 = '''SELECT "Title", "News_link" FROM sun_data WHERE "News_type" = 'Good News' LIMIT 5;'''
        cursor.execute(sql1)
        for top_5_good in cursor.fetchall():
            e.append(top_5_good)
        e.append(('there are no good news','if nothing is displayed else '))

        sql2 = '''SELECT "News_type", count(*) FROM sun_data WHERE "News_type" is not null GROUP BY "News_type";'''
        cursor.execute(sql2)
        for news_type in cursor.fetchall():
            a.append(news_type)

        sql4 = '''SELECT word, count(*) AS ct FROM sun_data, 
                    unnest(string_to_array("Full_content", ' ')) 
                    word GROUP  BY 1 ORDER  BY 2 DESC LIMIT  100;'''
        cursor.execute(sql4)
        for most_word in cursor.fetchall():
            b.append(most_word)    



        conn.close()

        news_t = pd.DataFrame(a)
        news_t.columns = ['type','count']
        news_t = news_t.plot(x = 'type',y="count",color=['red','green','grey'],kind="barh",title="Sentiment of The Sun Newspaper",figsize=(10,7))
        news_t.figure.savefig('sentiment_sun.png')


        good_news = pd.DataFrame(e)
        good_news.columns = ['title','link']
        good_news_sun = good_news.set_index('title')

        most_words = pd.DataFrame(b)
        most_words.columns = ['word','count']
        stops =  list(stopwords.words('english'))
        most_words = most_words[~most_words['word'].isin(stops)]
        most_words = most_words[~most_words['word'].str.isnumeric()]
        most_words = most_words.tail(20)
        most_words = most_words.plot(x = 'word',y="count",kind="barh",title="Most Frequent Words in the Sun",figsize=(10,7))
        most_words.figure.savefig('most_words_sun.png')
        return good_news_sun
        
    good_news_punch=punch_analytics()
    good_news_guardian=guardian_analytics()
    good_news_nation=nation_analytics()
    good_news_vanguard=vanguard_analytics()
    good_news_sun=sun_analytics()


# In[ ]:


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def email_update():
    import datetime
    now = datetime.datetime.now()

    current_time = now.strftime("%H:%M")
    today = datetime.date.today()


    email_sender = 'you_senders_email'
    email_password = 'email_notification_password'
    email_receiver = ['yourboss@gmail.com','yourcolleague@gmail.com','theannoyingintern@gmail.com']

    subject = "News Summary update for the last 3 hours on {}".format(today)
    body = '''Latest news update from 5 news online outlets, 
    \n Here are the good news based on the number of positive words in the full content 
    \n{good_punch}
    \n{good_guardian}
    \n{good_sun}
    \n{good_nation}
    \n{good_vanguard}
    \n and attached to this mail are the sentiments of the News for each news outlet
    '''.format(good_punch = good_news_punch,good_guardian = good_news_guardian,good_sun = good_news_sun,good_nation = good_news_nation,good_vanguard = good_news_vanguard)

    em = EmailMessage()
    em['From'] = email_sender
    em['To'] = email_receiver
    em['subject'] = subject
    em.set_content(body)

    folder =r'path to png pictures folder'
    for images in os.listdir(folder):

        # check if the image ends with png
        if (images.endswith(".png")):

            with open(images, 'rb') as f:
                img_data = f.read()
                file_name = f.name


            em.add_attachment(img_data,maintype = 'image',subtype='png',filename = file_name)

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context = context) as smtp:
        smtp.login(email_sender,email_password)
        smtp.sendmail(email_sender,email_receiver, em.as_string())


# ## Automation and Scheduling with PREFECT
# #### Runs every 3 hours
import datetime
def flow_caso(schedule=None):
  
   with Flow("news_pipeline",schedule=schedule) as flow:
       
       Punch_news = punch_news()
       Vanguard_news = vanguard_news()
       The_nation_news = the_nation()
       The_guardian = the_guardian()
       The_Sun = the_sun()   
       Basic_Analytics = database_interaction()
       Email_Notification = email_update()       

   return flow


schedule = IntervalSchedule(
   start_date = datetime.datetime.now() + datetime.timedelta(seconds = 2),
   interval = datetime.timedelta(hours=3)
)
flow=flow_caso(schedule)

flow.run()

