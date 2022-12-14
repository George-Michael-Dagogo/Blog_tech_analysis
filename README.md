# Nigerian news review

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
### * The script was automated and scheduled with Prefect to run every 3 hours.
### * This was hosted on an AZURE VM.


## Phase 2
### * Checks the most used word of the day.
### * Sends a summary to an email or several when the code is run
