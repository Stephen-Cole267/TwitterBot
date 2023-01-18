#####################################################################################
#####################################################################################
############################ SCRAPING TWEETS WITH TWEEPY ############################
#####################################################################################
#####################################################################################

from tweepy import OAuth1UserHandler
import tweepy
import pandas as pd
from datetime import datetime, timedelta
import os
import time
from decouple import config

#####################################################################################
################################ CREDENTIALS ########################################
#####################################################################################

API_KEY = config("API_KEY")
API_KEY_SECRET = config("API_KEY_SECRET")
ACCESS_TOKEN = config("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = config("ACCESS_TOKEN_SECRET")

#####################################################################################
############################### CONNECT TO API ######################################
#####################################################################################

auth = OAuth1UserHandler(
    consumer_key=API_KEY,
    consumer_secret=API_KEY_SECRET,
    access_token=ACCESS_TOKEN,
    access_token_secret=ACCESS_TOKEN_SECRET,
)
api = tweepy.API(auth, wait_on_rate_limit=True)
search_words = "fatalities OR armageddon OR deluge OR war OR damage OR death OR protests OR terrorist OR activist"
# Only allowed 450 requests / 15 mins
# https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets

# Due to the limited number of API calls on a free account, I am only able to extract 5,000 tweets per run every 15 minutes

#####################################################################################
############################### SCRAPE TWEETS #######################################
#####################################################################################


def scrape_tweets(search_words, numRuns, until=datetime.now().date(), numTweets=5000):
    """
    A function that will extract tweets that is related to the search parameter and store them in a csv file. This data will then be used to perform NLP
    NOTE: A timer of 15 minutes has been added to the end of this script to account for the API limit to number of requests.
    Args:
        search_words (string): String of keywords or hashtags for search parameter of interest
        numRuns (integer): number of runs that happen once per 15 minutes
        TODO: Update this arg: until (date): Starting date after which all tweets would be extracted NOTE: Can only extract tweets that are no longer than 7 days old
        numTweets (integer): number of tweets to pull per run
    Output:
        CSV File (pandas.DataFrame): A DataFrame of scraped tweets, based off of search_words arg, stored as a .csv file in /data directory
    """
    tweet_df = pd.DataFrame(
        columns=[
            "username",
            "acctdesc",
            "location",
            "following",
            "followers",
            "totaltweets",
            "usercreatedts",
            "tweetcreatedts",
            "retweetcount",
            "text",
            "hashtags",
        ]
    )
    for i in range(0, numRuns):
        start_time = time.time()
        print("start_time", time.asctime(time.localtime(time.time())))

        for tweet in tweepy.Cursor(
            api.search_tweets,
            q=search_words,
            lang="en",
            tweet_mode="extended",
            until=until,
            count=100,
        ).items(numTweets):

            username = tweet.user.screen_name  # Twitter handler
            acctdesc = tweet.user.description  # Description of account
            location = tweet.user.location  # Where user is tweeting from
            following = (
                tweet.user.friends_count
            )  # No. of other users that the twitter handler is following
            followers = (
                tweet.user.followers_count
            )  # No. of other users that are following the twitter handler
            totaltweets = tweet.user.statuses_count  # Total tweets by twitter handler
            usercreatedts = (
                tweet.user.created_at
            )  # When the twitter handler was created
            tweetcreatedts = tweet.created_at  # When the tweet was created
            retweetcount = tweet.retweet_count  # No. of retweets
            hashtags = tweet.entities["hashtags"]  # Hashtags in the tweet

            try:
                text = tweet.retweeted_status.full_text  # Full text of the tweet

            except AttributeError:  # Not a Retweet
                text = tweet.full_text  # Full text of the tweet that isn't a retweet
            # Store each tweet's features in a list for DataFrame imputation
            ith_tweet = [
                username,
                acctdesc,
                location,
                following,
                followers,
                totaltweets,
                usercreatedts,
                tweetcreatedts,
                retweetcount,
                text,
                hashtags,
            ]
            # Add tweet to DataFrame
            tweet_df.loc[len(tweet_df)] = ith_tweet

        end_time = time.time()
        total_time = end_time - start_time
        print(
            "{} tweets extracted in run {}, elapsed time: {}".format(
                tweet_df.shape[0], i + 1, str(timedelta(seconds=total_time))
            )
        )
        if numRuns != 1:
            time.sleep(920)  # 15 minute wait timer for request limit to refresh
        else:
            pass
    # Save DataFrame as .csv
    csv_timestamp = datetime.today().strftime("%Y%m%d_%H%M%S")
    file_name = os.path.join(
        config("DATA_PATH"), 
        "{}_disasterTweets.csv".format(csv_timestamp)
    )
    tweet_df.to_csv(file_name, index=False)
    print("SCRAPING COMPLETE")


# Run function
scrape_tweets(search_words=search_words, numRuns=10)
