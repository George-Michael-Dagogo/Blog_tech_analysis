# Online News Sentimental Analysis
# The primary goal of this project was to leverage web-scraping techniques to collect news data from five prominent online news outlets. Through advanced data cleaning and transformation techniques, we aimed to conduct sentiment analysis, categorize the data into positive, negative, and neutral news, and create insightful visualizations to better understand the underlying patterns and trends. 
# To further enhance the project's value, we incorporated an automated email delivery feature to disseminate the visualized data to several email addresses. 

## Architecture
![alt text](https://github.com/George-Michael-Dagogo/News_station_analysis/blob/main/NEWS.jpg)

## Webscrapes data from the below stated Nigerian online news platforms

### 1. https://punchng.com/   The Punch Newspaper         :heavy_check_mark:

### 2. https://www.vanguardngr.com/    Vanguard Newspapaer       :heavy_check_mark:

### 3. https://thenationonlineng.net/    The Nation Newspaper       :heavy_check_mark:

### 4. https://guardian.ng/     The Guardian Newspaper            :heavy_check_mark:

### 5. https://www.sunnewsonline.com/     The Sun News          :heavy_check_mark:

##
##
>

## Phase 1
### * The script Checks if a particular news is positive, negative or neutral.
### * An AZURE single postgres database server was used to store the webscraped data.
### * Each news outlet has its table in the Postgres database.
### * Any records older than 2 days will be deleted from each database table.
### * Duplicate values will also removed from each database table.


## Phase 2
### * Queries the database to get the count of each news sentiment, visualize it and save as png.
### * Checks the most used word of the day, visualize it and save as png.
### * Checks for the Author with the most count, visualize it and save as png.
### * Gets a list of the news types with the sentiment 'Good News'. 
### * Sends a summary to an email or several.

## Phase 3
### * The script was automated and scheduled with Prefect to run every 3 hours.
### * This was hosted on an AZURE VM.
