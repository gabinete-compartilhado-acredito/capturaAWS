import urllib.request, urllib.parse, urllib.error
import oauth
import json
import datetime
import boto3


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

    
def augment(url, parameters, credentials):
    consumer = oauth.OAuthConsumer(credentials['api_key'], credentials['api_secret_key'])
    token    = oauth.OAuthToken(credentials['access_token'], credentials['access_token_secret'])

    oauth_request = oauth.OAuthRequest.from_consumer_and_token(consumer,
                    token=token, http_method='GET', http_url=url,
                    parameters=parameters)
    oauth_request.sign_request(oauth.OAuthSignatureMethod_HMAC_SHA1(),
                               consumer, token)
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
    '''Convert a twitter created_at string to a datetime object'''

    date_time_str = date

    date_time_obj = datetime.datetime.strptime(date_time_str, '%a %b %d %H:%M:%S +0000 %Y')

    return str(date_time_obj.date())


def compare_dates(data, str_date, end_date):
    
        date = string_to_date(data['created_at'])        
           
        if (date <= end_date) and (date >= str_date):            
            return True


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