import requests
from lxml import html
import json
import datetime as dt

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

def entrypoint(params):
    """
    Input:   params (dict)
             end_date, date_format, timedelta, secao
    Retorna: lista de dicts com url e path
    """
    # Hard-coded:
    url_prefix = 'http://www.in.gov.br/web/dou/-/'
        
    # Pega parâmetros de input:
    if params['end_date'] == 'now':
        end_date = dt.datetime.utcnow() + dt.timedelta(hours=-3) # Brasilia local time.
    elif params['end_date'] == 'yesterday':
        end_date = dt.datetime.utcnow() + dt.timedelta(hours=-3) + dt.timedelta(days=-1)        
    else:
        end_date = dt.datetime.strptime(params['end_date'], params['date_format'])
    timedelta = dt.timedelta(days=params['timedelta'])
    secoes = params['secao']
    secoes = [1, 2, 3, 'e', '1a'] if secoes == 'all' else secoes
    secoes = secoes if type(secoes) == list else [secoes]
    
    # LOOP sobre datas:
    forms = []
    start_date = end_date + timedelta
    for date in daterange(start_date, end_date + dt.timedelta(days=1)):
        # LOOP sobre seções do DO:
        for s in secoes:
            jsons = get_artigos_do(date, s)
            # LOOP sobre artigos daquele dia e seção:
            for j in jsons:
                url      = url_prefix + j['urlTitle']
                filename = date.strftime('%Y-%m-%d') + '_s' + str(s) + '_' + fix_filename(j['urlTitle']) + '.json'
                forms.append({'url':url, 'filename':filename})
    
    return forms

if __name__ == '__main__':
    pass