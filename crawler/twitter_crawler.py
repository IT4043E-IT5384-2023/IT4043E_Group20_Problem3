from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import csv


#Config--------------------
#Twitter username
Username = ""
#Twitter password
Password = ""
#URL of crawled site
URL = "https://twitter.com/search?q=%23crypto&src=typed_query"
#Number of scroll down refreshing
numIter = 10
#Interval between 2 Iteration
iterInterval = 5
#--------------------------


with open('twitter.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["UserTag", "Time", "Tweet", "imageUrl", "Reply", "reTweet", "Like", "View"])

file = open('twitter.csv', 'a', newline='')
writer = csv.writer(file)

options = webdriver.FirefoxOptions()
driver = webdriver.Firefox(options=options)

driver.get("https://twitter.com/login")

time.sleep(5)
username = driver.find_element(By.XPATH,"//input[@name='text']")
username.send_keys(Username)
next_button = driver.find_element(By.XPATH,"//span[contains(text(),'Next')]")
next_button.click()

time.sleep(5)
password = driver.find_element(By.XPATH,"//input[@name='password']")
password.send_keys(Password)
log_in = driver.find_element(By.XPATH,"//span[contains(text(),'Log in')]")
log_in.click()

time.sleep(5)
driver.get(URL)

# UserTags = []
# Times = []
# Tweets = []
# Replys = []
# reTweets = []
# Likes = []
# Views = []
# imageUrls = []

time.sleep(5)
for i in range(numIter):
    try:    
        articles = driver.find_elements(By.XPATH, "//article[@data-testid='tweet']")
    except:
        print("Iter {}/{}: Lagging catched, waiting.".format(i + 1, numIter))
        time.sleep(17)
        continue

    for article in articles:
        UserTag = ""
        try:
            driver.implicitly_wait(5)
            UserTag = article.find_element(By.XPATH, ".//div[@data-testid='User-Name']").text
            # UserTags.append(UserTag)
        except:
            pass
    
        Time = ""
        try:
            driver.implicitly_wait(5)
            Time = article.find_element(By.XPATH, ".//time").get_attribute("datetime")
            # Times.append(Time)
        except:
            pass
    
        Tweet = ""
        try:
            driver.implicitly_wait(5)
            Tweet = article.find_element(By.XPATH, ".//div[@data-testid='tweetText']").text
            # Tweets.append(Tweet)
        except:
            pass
    
        imageUrl = ""
        try:
            driver.implicitly_wait(5)
            imageUrl = article.find_element(By.XPATH, ".//div[@data-testid='tweetPhoto']/img").get_attribute("src")
            # imageUrls.append(imageUrl)
        except:
            pass
    
        Reply = ""
        try:
            driver.implicitly_wait(5)
            Reply = article.find_element(By.XPATH, ".//div[@data-testid='reply']").text
            # Replys.append(Reply)
        except:
            pass
    
        reTweet = ""
        try:
            driver.implicitly_wait(5)
            reTweet = article.find_element(By.XPATH, ".//div[@data-testid='retweet']").text
            # reTweets.append(reTweet)
        except:
            pass
    
        Like = ""
        try:
            driver.implicitly_wait(5)
            Like = article.find_element(By.XPATH, ".//div[@data-testid='like']").text
            # Likes.append(Like)
        except:
            pass

        View = ""
        try:
            driver.implicitly_wait(5)
            View = article.find_element(By.XPATH, ".//a[@role='link']/div/div[2]/span/span/span").text
            # Views.append(View)
        except:
            pass

        writer.writerow([UserTag, Time, Tweet, imageUrl, Reply, reTweet, Like, View])

    # driver.execute_script('window.scrollTo(0,document.body.scrollHeight);')
    print("Iter {}/{}: Retrieving succeed.".format(i + 1, numIter))
    driver.execute_script('window.scrollBy(0,3200);')
    time.sleep(iterInterval)

file.close()

time.sleep(30)
driver.quit()
