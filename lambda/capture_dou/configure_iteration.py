import global_settings as gs
import datetime as dt

def brasilia_day():
    """
    No matter where the code is ran, return UTC-3 day
    (Brasilia local day, no daylight savings)
    """
    return (dt.datetime.utcnow() + dt.timedelta(hours=-3)).replace(hour=0, minute=0, second=0, microsecond=0)

def format_date(config):
    pass

def update_config(config, end_date):
    """
    Given a config file for capturing DOU articles' URLs and the number of
    articles that sgould be downloaded prior to batch size limitations `Nurls`,
    return an updated config for the next request try. 
    
    Required config keys:
    * end_date    > The articles' date to request the URLs;
    * date_format > The format of the date above (e.g. %Y-%m-%d);
    * timedelta   > Current implementation requires this to be 0;
    * url_list    > filename (or DynamoDB table name) where a list of captured URLs is stored;
    * daily_clean_url_list > Whether or not to erase the content of url_list once the current day change.
    """
    
    if config['timedelta'] != 0:
        raise Exception('current implementation only allows timedelta=0.')
    
    # Copy config:
    config2  = dict(config)

    if config['daily_clean_url_list'] == True:
        raise Exception('current implementation does not allow daily_clean_url_list=True.')

    # Update end_date:
    config2['end_date'] = (end_date + dt.timedelta(days=1)).strftime(config['date_format'])
    return config2


def configure_iteration(config):
    """
    Get as input a dict 'config' with keys:
    
    * 'date_format': format of 'end_date' below, e.g. '%Y-%m-%d';
    * 'end_date':    last date to search for URLs (one can set to 'now' to get the current day); 
    * 'secao':       list of DOU sections to scan (1, 2, 3, e and/or 1a, or set to 'all' for '[1,2,3,e]';
    * 'timedelta':   number of days from end_date to start URL search (is a negative number);
    * 'url_list':    filename or dynamoDB table name of a list of captured URLs (to avoid capturing again).
    * 'daily_clean_url_list': whether or not to erase 'url_list' every day.

    and creates a list of DOU articles' URLs to download. 
    """
    
    # Debug message:
    if True or gs.debug == True:
        print("Starting configure_iteration with config:")
        print(config)
    
    # Translate string representing date to datetime:
    if gs.debug == True:
        print('Reading date range...')
    if config['end_date'] == 'now':
        end_date = brasilia_day()
    elif config['end_date'] == 'yesterday':
        end_date = brasilia_day() + dt.timedelta(days=-1)
    else:
        end_date = dt.datetime.strptime(config['end_date'], config['date_format'])

    timedelta = dt.timedelta(days=config['timedelta'])
    # If end_date is in the future, return empty list and same config
    # (wait for the next day):
    # PS: this will skip request URLs even for negative timedelta.
    if end_date > brasilia_day():
        skip = True
        return skip, config
    skip = False

    # Translate secao config to a list of strings:
    if gs.debug == True:
        print('Reading selected sections...')    
    secoes = config['secao']
    secoes = ['DO1', 'DO2', 'DO3', 'DO2E', 'DO1E'] if secoes == 'all' else secoes
    secoes = secoes if type(secoes) == list else [secoes]
    config['secao'] = secoes
            
    return skip, update_config(config, end_date)
