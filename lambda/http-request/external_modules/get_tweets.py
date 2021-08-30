import urllib.request, urllib.parse, urllib.error
import json
import datetime
import time
import boto3
import oauth
import twitter as xt


def compute_sleep_time(requests_per_window, max_frac=0.90, window_min=15):
    """
    Return a minimum time interval between requests to 
    avoid violating twitter API rules.
    
    Input
    -----
    
    requests_per_window : int
        Maximum number of requests allowed per time window.
        Check https://developer.twitter.com/en/docs/twitter-api/v1 
        for values.
    
    max_frac : float
        Factor from 0.0 to 1.0 to multiply `requests_per_window`
        to derive a safer maximum number of requests per window
        (with a buffer from the true limit).
    
    window_min : int or float
        Number of minutes correponding to one time window interval
        (tipically 15 minutes).
    
    Return
    ------
    
    sleep_time : float
        Number of seconds to wait between two consecutive
        API calls.
    """
    
    window_sec = window_min * 60
    max_req_per_window = int(max_frac * requests_per_window)
    sleep_time = window_sec / max_req_per_window
    
    return sleep_time


def get_s3_json(bucket, key):
    """
    Load JSON file stored in AWS S3 as a dict, and return it.
    """
    
    # Load credentials from AWS S3:
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    # Translate S3 object content to a dict:
    string_content = response['Body'].read().decode('utf-8')
    dict_content = json.loads(string_content)
    
    return dict_content

    
def augment(url, parameters, credentials, http_method='GET'):
    """
    Create a URL for HTTP request with authentication 
    information. These are added as extra parameters.
    """
    
    # Create an object with the api key and secret key as attributes `key` and `secret`:
    consumer = oauth.OAuthConsumer(credentials['api_key'], credentials['api_secret_key'])
    # Create an object with the token and token secret as attributes; this object has to/from string methods: 
    token    = oauth.OAuthToken(credentials['access_token'], credentials['access_token_secret'])

    # Create an object with all the provided information plus OAuth version, timestamp and random number:
    oauth_request = oauth.OAuthRequest.from_consumer_and_token(consumer, token=token, http_method=http_method, http_url=url, parameters=parameters)
    # Create the attribute 'oauth_signature' in the object; this attribute is a authentication signature built from my secret key and the full message to be sent:
    oauth_request.sign_request(oauth.OAuthSignatureMethod_HMAC_SHA1(), consumer, token)

    return oauth_request.to_url()
    

def captura_de_tweets(id, credentials, max_id=None):
    """
    Given an twitter user `id`, returns the last 200 tweets before the tweet `max_id`.
    If `max_id` is None (default), return the most recent 200 tweets.
    """

    if max_id == None:
        pars = {'user_id': id, 'count':200, 'tweet_mode':'extended'}
    else:
        pars = {'user_id': id, 'count':200, 'max_id': max_id, 'tweet_mode':'extended'}

    url = augment('https://api.twitter.com/1.1/statuses/user_timeline.json', pars, credentials)  

    connection = urllib.request.urlopen(url)
    data = connection.read().decode()
    js = json.loads(data)

    return js
    

def string_to_date(date):
    """
    Convert a twitter created_at string to a datetime object.
    """

    date_time_str = date

    date_time_obj = datetime.datetime.strptime(date_time_str, '%a %b %d %H:%M:%S +0000 %Y')

    return str(date_time_obj.date())


def compare_dates(tweet, str_date, end_date):
        """
        Return True if `tweet` (dict) happened is inside the 
        inclusive date interval defined by `str_date` and 
        `end_date` (given in format '%Y-%m-%d').  
        """
        
        date = string_to_date(tweet['created_at'])        
           
        if (date <= end_date) and (date >= str_date):            
            return True
        else:
            return False


def get_tweets(id, str_date, end_date):
    
    ''' returns twittes from a user since start_date to end_date limited to 3.200 twittes'''
    
    # Load twitter OAuth token from AWS S3:
    credentials = get_s3_json('config-lambda', 'gabitwitter.json')
    
    data = captura_de_tweets(id, credentials)
    date = string_to_date(data[-1]['created_at']) # formata a data do post mais antigo em string    
    while date >= str_date:       
    
        max_id = data[-1]['id'] # pega o id do post mais antigo              
        data += captura_de_tweets(id, credentials, max_id) 
        date = string_to_date(data[-1]['created_at'])
        
    # Adiciona a lista de twites apenas os twittes dentro da data especificada          
    dados = list(filter(lambda x: compare_dates(x, str_date, end_date), data))  
    
    return dados 
    
    
def get_list_tweets(list_id, str_date, end_date, requests_per_window=900, verbose=False):
    """
    Returns tweets posted in Twitter list `list_id` (int) 
    since `str_date` up to `end_date` (note that there is a
    tweet age limit to the Twiter API). 
    
    To avoid violaring Twitter API request limits, set 
    `requests_per_window` to the API limit.
    """
    
    # Load twitter OAuth token from AWS S3:
    if verbose:
        print('Loading credentials...')
    credentials = get_s3_json('config-lambda', 'twitter_compartwi.json')
    
    # Config for API requests:
    endpoint = 'https://api.twitter.com/1.1/lists/statuses.json'
    pars = {'list_id': list_id, 'count': 200, 'include_entities': True, 'include_rts': True, 'tweet_mode': 'extended'}

    # Get tweets in list: 
    if verbose:
        print('Making the first API request...')
    data = xt.request_twitter_api(endpoint, pars, credentials=credentials)
    if verbose:
        print('# tweets: {}'.format(len(data)))
    # Set max_id to skip already captured tweets:
    pars['max_id'] = data[-1]['id'] - 1
    
    if verbose:
        print('Prior do loop:')
    # Get date of oldest captured tweet:
    date = string_to_date(data[-1]['created_at'])
    while date >= str_date:       
        
        if verbose:
            print('LOOP: current capture <date> <tweet_id>: {} {}'.format(date, pars['max_id']))
        
        # Sleep to avoid violating Twitter maximum request rates:
        if verbose:
            print('LOOP: sleeping...')
        sleep_time = compute_sleep_time(requests_per_window, window_min=15)
        time.sleep(sleep_time)
        
        # Configure the request to start from the oldest tweet:
        if verbose:
            print('LOOP: making API request...')
        new_data = xt.request_twitter_api(endpoint, pars, credentials=credentials) 
        if verbose:
            print('# tweets: {}'.format(len(new_data)))

        # Exit loop if requests do not return new data (probably due to limit in API historical response):
        if len(new_data) == 0 or new_data[-1]['id'] - 1 == pars['max_id']:
            break
        
        # Update data to continue loop:
        data += new_data
        pars['max_id'] = data[-1]['id'] - 1
        date = string_to_date(data[-1]['created_at'])
        
        
    # Adiciona a lista de twites apenas os twittes dentro da data especificada
    if verbose:
        print('Filtering results by date...')
    dados = list(filter(lambda x: compare_dates(x, str_date, end_date), data))  
    
    return dados 