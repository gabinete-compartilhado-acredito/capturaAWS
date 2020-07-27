import get_tweets as gt

def entrypoint(response, event):
    """
    Gets tweets and create a list of dicts with them.
    
    Input
    -----
    
    event : dict
        A dict containing all the required info to get the tweets.
        
    response : anything
        This particular module does not use `response`.
        
    Returns
    -------
    
    tweet_list : list of dicts
        All the information about the tweets.
    """
        
    tweet_list = gt.get_tweets(int(event['identifier']), event['aux_data']['data_ini'], event['aux_data']['data_fim'])
    
    return tweet_list
