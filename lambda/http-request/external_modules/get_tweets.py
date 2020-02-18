import GetOldTweets3 as got
from datetime import datetime, timedelta

def get_tweets(usuario, data_max, data_min=None):   
    """
    Get all tweets posted between specified dates from a user.
    
    Input
    -----
    
    usuario : str
        The twitter username to inspect.
    
    data_max : str 
        Last date (inclusive) to search for tweets, in '%Y-%m-%d' format.
        It can also be 'yesterday'.
    
    data_min : str 
        First date (inclusive) to search for tweets, in '%Y-%m-%d' format.
        It can also be 'yesterday' or `None`, in which case it extracts all tweets.
        
    Returns
    ------- 
    
    user_tweets : list of dicts
        Data from each tweet found for the user and in the date range.    
    """
    
    # Set last tweet date (input is inclusive, but until_date is exclusive):
    if data_max == 'yesterday':
        until_date = datetime.today().strftime('%Y-%m-%d')
    else:
        until_date = (datetime.strptime(data_max, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    
    if data_min == None:
        # Creation of query object:
        tweetCriteria = got.manager.TweetCriteria().setUsername(usuario).setUntil(until_date)
    else:
        if data_min == 'yesterday':
            since_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d') # since_date is inclusive.
        else:
            since_date = data_min            
        # Creation of query object:
        tweetCriteria = got.manager.TweetCriteria().setUsername(usuario).setSince(since_date).setUntil(until_date)
        
    # Creation of list that contains all tweets
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    
    # Creating list of chosen tweet data
    user_tweets = [{'author_id': tweet.author_id, 'tweet_id': tweet.id, 'date': tweet.formatted_date, 
                    'username': tweet.username, 'text': tweet.text,'geo':tweet.geo, 'likes':tweet.favorites,
                    'hashtags':tweet.hashtags, 'mentions':tweet.mentions, 'tweet_link': tweet.permalink,
                    'replies':tweet.replies, 'retweets': tweet.retweets, 'to':tweet.to, 'url':tweet.urls} for tweet in tweets]
    
    return user_tweets
