# Importing necessary libraries
import boto3
import pymysql.cursors
import requests
from bs4 import BeautifulSoup

############################################################################################################
# Function to find RSS feed of a News URL
def find_rss(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    all_links = soup.find_all()

    rss_feed = None
    for link in all_links:
        if 'href' in link.attrs:
            if 'rss' in link['href']:
                rss_feed = link['href']
    return rss_feed


# Function to get category-wise RSS feed URL from an RSS feed
def get_rss_category_and_links(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        all_links = soup.find_all('a')
        rss_links = {}

        for link in all_links:
            if 'rss' in link.get('href', '') or 'rss' in link.text.lower():
                rss_links[link.text.strip()] = link['href']

        rss_category_list = []
        rss_link_list = []
        for category, rss_link in rss_links.items():
            rss_category_list.append(category)
            rss_link_list.append(rss_link)

        return rss_category_list, rss_link_list

    except requests.RequestException as e:
        print(f"An error occurred: {str(e)}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return None


# Function to get title_link and published date
def get_title_published(rss_feed_url):
    r = requests.get(rss_feed_url)
    soup = BeautifulSoup(r.text, 'html.parser')

    title_links = soup.find_all('item')
    links = []
    published_dates = []

    for item in title_links:
        links.append(item.guid.text)
        published_dates.append(item.pubdate.text)
    return links, published_dates


# Function to get title, description and articleBody using title_url
def news_title_info(title_url):
    from goose3 import Goose
    g = Goose()
    article = g.extract(title_url)
    title = article.title
    description = article.meta_description
    body = article.cleaned_text

    g.close()
    return title, description, body


# Function to analyze sentiment using AWS Comprehend
def analyze_sentiment(description):
    if not description:
        return None, None

    try:
        response = comprehend.detect_sentiment(Text=description, LanguageCode='en')
        sentiment = response['Sentiment']
        score = response['SentimentScore'][sentiment.capitalize()]
        return sentiment, score

    except Exception as e:
        print("Error analyzing sentiment:", e)
        return None, None


############################################################################################################
# Connection to AWS Comprehend
try:
    session = boto3.Session(aws_access_key_id="",
                            aws_secret_access_key="",
                            region_name='')
    comprehend = session.client("comprehend")
    print("Connection to AWS Comprehend Established Successfully!!!")
except Exception as e:
    print(e)

############################################################################################################
# Connect to the database
try:
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='password',
                                 database='Sentiment_Analysis',
                                 cursorclass=pymysql.cursors.DictCursor)
    print("Connection to the database established successfully!")

except pymysql.Error as e:
    print(f"Error connecting to MySQL database: {e}")
############################################################################################################

with connection:
    with connection.cursor() as cursor:
        cursor.execute("USE Sentiment_Analysis")
        # Create the rss_info table if not exists
        query1 = '''CREATE TABLE IF NOT EXISTS `rss_info` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `news_url` VARCHAR(500),
                    `rss_feed` VARCHAR(500),
                    `rss_category` VARCHAR(100),
                    `rss_feed_url` VARCHAR(500),
                    `updated_time_stamp` DATETIME,
                    UNIQUE KEY `unique_rss_info` (`rss_feed_url`)
                  )'''
        cursor.execute(query1)

        # Fetching news_url from client_table where active_flag is 1
        cursor.execute("SELECT news_url FROM client_table WHERE active_flag = 1")
        active_news_urls = cursor.fetchall()

        # Inserting fetched news_urls into rss_info table along with their RSS feeds
        for row in active_news_urls:
            news_url = row['news_url']

            if news_url:
                rss_feed = find_rss(news_url)  # Extract RSS feed from news_url

                if rss_feed:
                    rss_category, rss_feed_url = get_rss_category_and_links(rss_feed)
                    if rss_category and rss_feed_url:
                        for category, feed_url in zip(rss_category, rss_feed_url):
                            # Check if the record already exists based on rss_feed_url
                            select_query = "SELECT * FROM `rss_info` WHERE `rss_feed_url` = %s"
                            cursor.execute(select_query, (feed_url,))
                            existing_rss_feed_url = cursor.fetchone()

                            if not existing_rss_feed_url:
                                # Insert the record if it doesn't already exist
                                insert_query1 = '''INSERT INTO `rss_info` (`news_url`, `rss_feed`, `rss_category`, `rss_feed_url`, `updated_time_stamp`)
                                                  VALUES (%s, %s, %s, %s, NOW())'''
                                cursor.execute(insert_query1, (news_url, rss_feed, category, feed_url))

        connection.commit()
        print("Table 'rss_info' Updated Successfully !!!")
        ############################################################################################################

        # Create the news_info table if not exists
        query2 = '''CREATE TABLE IF NOT EXISTS `news_info` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `rss_feed_url` VARCHAR(500),
                    `title_url` VARCHAR(500),
                    `published_date` VARCHAR(100),
                    `active_flag` INT,
                    UNIQUE KEY `unique_news_info` (`title_url`)
                  )'''
        cursor.execute(query2)

        # Fetching rss_feed_url from 'rss_info'
        cursor.execute("SELECT rss_feed_url FROM `rss_info` WHERE `rss_category` = 'Top Stories' ")
        rss_category_url = cursor.fetchall()

        for row in rss_category_url:
            rss_feed_url = row['rss_feed_url']

            if rss_feed_url:
                links, published_dates = get_title_published(rss_feed_url)

                if links:
                    for title_link, published_date in zip(links, published_dates):
                        # Check if the record already exists based on title_url
                        select_query = "SELECT * FROM `news_info` WHERE `title_url` = %s"
                        cursor.execute(select_query, (title_link,))
                        existing_record = cursor.fetchone()

                        if not existing_record:
                            # Insert the record if it doesn't already exist
                            insert_query2 = '''INSERT INTO `news_info` (`rss_feed_url`, `title_url`, `published_date`, `active_flag`)
                                              VALUES (%s, %s, %s, %s)'''
                            cursor.execute(insert_query2, (rss_feed_url, title_link, published_date, 1))
        connection.commit()
        print("Table `news_info` Updated Successfully!!")
        #######################################################################################################################
        # Create the sentiment_results table if not exists
        query3 = '''CREATE TABLE IF NOT EXISTS `sentiment_results` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `title_url` VARCHAR(500),
                    `title` TEXT,
                    `description` LONGTEXT,
                    `body` LONGTEXT,
                    `sentiment` TEXT,
                    `sentiment_score` FLOAT
                  )'''
        cursor.execute(query3)

        # Fetching title_url from 'news_info' table
        cursor.execute(
            "SELECT t3.title_url FROM `news_info` t3 WHERE t3.title_url NOT IN (SELECT t4.title_url FROM `sentiment_results` t4)")
        active_title_urls = cursor.fetchall()

        for row in active_title_urls:
            title_url = row['title_url']

            if title_url:
                title, description, body = news_title_info(title_url)

                if description:
                    sentiment, sentiment_score = analyze_sentiment(description)

                    insert_query3 = """INSERT INTO `sentiment_results` (`title_url`, `title`, `description`, `body`, `sentiment`, `sentiment_score`)
                    VALUES (%s, %s, %s, %s, %s, %s)"""
                    cursor.execute(insert_query3, (title_url, title, description, body, sentiment, sentiment_score))

    connection.commit()
    print("Table sentiment_results Updated Successfully!!")