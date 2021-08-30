import json
import requests

import oauth

    
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


def request_twitter_api(resource_url, parameters, method='GET', credentials='/home/skems/gabinete/projetos/keys-configs/twitter_compartwi.json'):
    """
    Make a request to a Twitter API endpoint and return the response
    as a parsed JSON (i.e. a list of dicts or dict of lists).
    
    Input
    -----
    
    resource_url : str
        The API endpoint URL address. It works with API v1, not sure 
        about v2. Check API reference under https://developer.twitter.com/en/docs/twitter-api/v1
        for available endpoints. Working for GET methods. Maybe POST methods
        require a change in the `oauth` code.
        
    parameter : dict
        The paremeters (keys) and their values (values) for the endpoint 
        request. Check the reference mentioned above for available 
        parameters.
    
    method : str
        The HTTP method to use. It can be 'GET' or 'POST' and it 
        should match the type of the endpoint specified in `resource_url`.
    
    credentials: dict or str
        if dict:
            Credentials generated when creating a project and an app
            at https://developer.twitter.com. The necessary ones in 
            this dict are: 'api_key', 'api_secret_key', 'access_token' 
            and 'access_token_secret'.
        if str:
            Filename of a JSON file containing the credentials as
            described above.
        
    Return
    ------
    
    data : dict
        The response from the API endpoint.
    """

    # Input and sanity checks:
    assert method == 'GET' or method == 'POST', "Unknown HTTP method {}; it should be 'GET' or 'POST'."
    
    # Check if credentials have been provided or if it must be loaded:
    cred_type = type(credentials)
    if cred_type != dict:
        if cred_type == str:
            # Load credentials from JSON file:
            with open(credentials, 'r') as f:
                credentials = json.load(f)
        else:
            raise Exception('Unknown `credentials` type `{}`.'.format(cred_type))

    # Create URL for rest API:
    url = augment(resource_url, parameters, credentials, method)

    # Prompt the API and get the response:
    if method == 'POST':
        response = requests.post(url)
    else:
        response = requests.get(url)

    # Get content if existent:
    if response.status_code != 200:
        raise Exception("Request failed ({}): {}.".format(response.status_code, response.reason))
    else:
        data = json.loads(response.content.decode('utf-8'))
        
    return data
