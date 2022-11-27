from selenium.webdriver import Chrome
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
import time
import random
import more_itertools as mit
import pandas as pd
import os
import requests

options = Options()
ua = UserAgent()
userAgent = ua.random
options.add_argument(f'user-agent={userAgent}')

#access to the path of your chromedriver.exe
driver = webdriver.Chrome('/workspace/News_station_analysis/chromedriver.exe')
#the homepage since you have to sign in first before navigating to the required page for sign in
driver.get("https://punchng.com/")
#waits until your page loads completely before sign in process begins
driver.implicitly_wait(20)