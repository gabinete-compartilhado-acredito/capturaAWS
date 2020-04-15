import requests
from lxml import html
import json
import datetime as dt
import boto3
from dynamodb_json import json_util as dyjson

debug = False

def daterange(start_date, end_date):
    """
    Same as python's 'range', but for datetime.
    NOTE: currently it does not support steps input.
    """
    for n in range(int ((end_date - start_date).days)):
        yield start_date + dt.timedelta(n)
        
def get_artigos_do(data, secao):
    """
    Para uma data (datetime) e uma seção (str) do DOU,
    retorna uma lista de jsons com todos os links e outros metadados dos 
    artigos daquele dia e seção. 
    """
    # Hard-coded:
    do_date_format = '%d-%m-%Y'
    # Transforma data:
    data_string = data.strftime(do_date_format)
    
    # Exemplo de URL: 'http://www.in.gov.br/leiturajornal?data=13-05-2019&secao=do1'
    url   = 'http://www.in.gov.br/leiturajornal?data=' + data_string + '&secao=do' + str(secao)

    # Specifies number of retries for GET:
    session = requests.Session()
    session.mount('http://www.in.gov.br', requests.adapters.HTTPAdapter(max_retries=3))
    
    # Captura a lista de artigos daquele dia e seção:
    res   = session.get(url)
    tree  = html.fromstring(res.content)
    xpath = '//*[@id="params"]/text()'
    return json.loads(tree.xpath(xpath)[0])['jsonArray']

def fix_filename(urlTitle):
    """
    Change the url 'urlTitle' substring used to acess the DOU article to something 
    that can be used as part of a filename.    
    """
    fixed = urlTitle.replace('//', '/')
    return fixed

def load_remote_config(table_name, key):
    """
    Given a hard-coded table reference in dynamoDB (AWS) (see event), 
    loads the configuration for the DOU articles' capture.
    """
    
    # Format key to dynamoDB json:
    key = {"name": {"S": key}}
    
    if debug:
        print('load_remote_config key:', key)
    
    # Read json from dynamoDB: 
    client   = boto3.client('dynamodb')
    response = client.get_item(TableName=table_name,Key=key)
    response = dyjson.loads(response)
    # Get configurations:
    config   = response['Item']
    
    return config


def brasilia_day():
    """
    No matter where the code is ran, return UTC-3 day
    (Brasilia local day, no daylight savings)
    """
    return (dt.datetime.utcnow() + dt.timedelta(hours=-3)).replace(hour=0, minute=0, second=0, microsecond=0)

def update_config(config, Narticles_in_section):
    """
    Given a config file for capturing DOU articles' URLs and a dict 
    that states how many articles were found in each requested section
    'Narticles_in_section', return an updated config for the next request 
    try. 
    
    Required config keys:
    * end_date    > The articles' date to request the URLs;
    * date_format > The format of the date above (e.g. %Y-%m-%d);
    * secao       > Current list of sections to request URLs;
    * secao_all   > All sections one may want to request (does not update);
    * timedelta   > Current implementation requires this to be 0.
    * last_extra  > The extra edition number of the last capture.
    """
    
    if config['timedelta'] != 0:
        raise Exception('current implementation only allows timedelta=0.')
    
    # Copy config:
    config2  = dict(config)
    end_date = dt.datetime.strptime(config['end_date'], config['date_format'])
            
    # If end_date is in the future, keep the same config:
    if end_date > brasilia_day():
        return config2
    
    # If end_date is in the past, return next day and all sections:
    if end_date < brasilia_day():
        config2['secao'] = config['secao_all']
        config2['end_date'] = (end_date + dt.timedelta(days=1)).strftime(config['date_format'])
        config2['last_extra'] = 0
        return config2
    
    # PRESENT DAY: find out missing sections and set config to that:
    # PS: always keep Extra ('e') because it can appear at any time 
    section_keys = list(filter(lambda k: Narticles_in_section[k] == 0 or k == 'e', Narticles_in_section.keys()))
    config2['secao'] = section_keys

    # If there are no missing sections, reset sections list and get next day:
    if len(section_keys)==0:
        config2['end_date'] = (end_date + dt.timedelta(days=1)).strftime(config['date_format'])
        config2['secao'] = config['secao_all']
        
    return config2

def get_articles_url(config):
    """
    Get as input a dict 'config' with keys:
    
    * 'date_format': format of 'end_date' below, e.g. '%Y-%m-%d';
    * 'end_date':    last date to search for URLs (one can set to 'now' to get the current day); 
    * 'secao':       list of DOU sections to scan (1, 2, 3, e and/or 1a, or set to 'all' for '[1,2,3,e]';
    * 'timedelta':   number of days from end_date to start URL search (is a negative number);
    
    and creates a list of DOU articles' URLs to download. 
    """
    
    # Hard-coded stuff:
    url_prefix = 'http://www.in.gov.br/web/dou/-/'
    
    # Debug message:
    if debug:
        print("Starting get_articles_url with config:")
        print(config)
    
    # Translate string representing date to datetime:
    if debug:
        print('Reading date range...')
    if config['end_date'] == 'now':
        end_date = brasilia_day()
    elif config['end_date'] == 'yesterday':
        end_date = brasilia_day() + dt.timedelta(days=-1)
    else:
        end_date = dt.datetime.strptime(config['end_date'], config['date_format'])
    # Save it back to config dict:
    config['end_date'] = end_date.strftime(config['date_format'])
    
    timedelta = dt.timedelta(days=config['timedelta'])
    
    # If end_date is in the future, return empty list and same config
    # (wait for the next day):
    # PS: this will skip request URLs even for negative timedelta.
    if end_date > brasilia_day():
        return [], config
        
    # Translate secao config to a list of strings:
    if debug:
        print('Reading selected sections...')    
    secoes = config['secao']
    secoes = [1, 2, 3, 'e', '1a'] if secoes == 'all' else secoes
    secoes = secoes if type(secoes) == list else [secoes]
    secoes = [str(s) for s in secoes]
    
    # LOOP over dates:
    url_file_list = []
    Narticles_in_section = dict(zip(secoes, [0]*len(secoes)))
    start_date = end_date + timedelta
    if debug:
        print('Will enter loop over config date and section range:')    
    for date in daterange(start_date, end_date + dt.timedelta(days=1)):
        if debug:
            print('-- '+date.strftime('%Y-%m-%d'))
        # LOOP over DOU sections:
        for s in secoes:
            if debug:
                print('   -- s'+str(s))
            jsons = get_artigos_do(date, s)
            Narticles_in_section[s] = len(jsons)
            # LOOP over downloaded URL list:
            if debug:
                print('      Looping over URLs...')            
            for j in jsons:
                url      = url_prefix + j['urlTitle']
                filename = date.strftime('%Y-%m-%d') + '_s' + str(s) + '_' + fix_filename(j['urlTitle']) + '.json'
                url_file_list.append({'url':url, 'filename':filename})
        
    if debug:
        print('Narticles_in_section:', Narticles_in_section)
    
    # Only update config if it is going to be saved in Dynamo later on.
    if config['update_config']:
        next_config = update_config(config, Narticles_in_section)
    else:
        next_config = config
        
    return url_file_list, next_config


def entrypoint(params):
    """
    Input:   params (dict)
             Com as keywords 'dynamo_table' e 'config_key'
    Retorna: lista de dicts com url e path
    
    Atualiza a config no dynamoDB
    """
    
    # Load config from dynamoDB:
    if params['use_config']:
        config = load_remote_config(params['dynamo_table'], params['config_key'])
        config['update_config'] = True
    # Or use directly supplied parameters:
    else:
        config = params
        config['update_config'] = False
        
    # Get list of articles to download and update config:
    url_file_list, next_config = get_articles_url(config)
    
    # Save config to AWS DynamoDB:
    if params['use_config']:
        client = boto3.client('dynamodb')
        response = client.put_item(TableName=params['dynamo_table'], Item=dyjson.dumps(next_config, as_dict=True))
    
    return url_file_list
    

if __name__ == '__main__':
    pass