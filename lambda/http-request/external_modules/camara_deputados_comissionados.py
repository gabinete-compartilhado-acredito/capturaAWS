import requests
from bs4 import BeautifulSoup


def html_table_to_dict_list(soup_table):
    """
    Input: 
      - 'soup_table': a Beatiful soup object that was selected by the tag 'table'.
      
    Returns a list of dicts containing the table's data. 
    It assumes the header is marked by 'th' tags.
    """
    trs = soup_table.find_all('tr')
    html_header  = trs[0].find_all('th')
    table_header = [t.text for t in html_header]
    return [dict(zip(table_header, [t.text for t in tr.find_all('td')])) for tr in trs[1:]]
    

def has_valueQ(remuneracao_dict):
    """
    Return TRUE if 'remuneracao_dict' has a key like 'valor'.
    Return FALSE otherwise.
    """
    return any([s.lower().find('valor') != -1 for s in remuneracao_dict.keys()])


def remove_accents(string, i=0):
    """
    Input: string
    
    Returns the same string, but without all portuguese-valid accents.
    """
    accent_list = [('ç','c'),('ã','a'),('á','a'),('à','a'),('â','a'),('é','e'),('ê','e'),('í','i'),('õ','o'),('ó','o'),
                   ('ô','o'),('ú','u')]
    if i >= len(accent_list):
        return string
    else:
        string = string.replace(*accent_list[i])
        return remove_accents(string, i + 1)


def parse_ptbr_number(string):
    """
    Input: a string representing a float number in Brazilian currency format, e.g.: 1.573.345,98
    
    Returns the corresponding float number.
    """
    number = string
    number = number.replace('.', '').replace(',', '.')
    return float(number)


def descricao_to_tag(descricao):
    """
    Input: a string 'descricao' describing a field.
    
    Returns a lower-case, underscore-separated, no-accent, first-two-words 
    version of the string, to be used as tag.
    """
    tag = descricao
    tag = tag.split('-')[-1]
    tag = tag.strip()
    tag = tag.lower()
    tag = remove_accents(tag)
    tag = tag.replace(' ou ',' ').replace(' de ',' ').replace(' em ', ' ')
    tag = tag.replace('/','X').replace('*', '')
    tag = tag.replace('(','').replace(')','')
    tag = '_'.join(tag.split(' ')[:2])
    
    return tag 


def structure_remuneracao(remuneracao_dict_list):
    """
    Input: a list of dicts where the values of the first and second fields are
           strings representing a description and a value in Brazilian currency format.
    
    Returns a dict with tag versions of the description as keys and the float of 
    the values as values.
    """
    remuneracao = list(filter(has_valueQ, remuneracao_dict_list))
    tags   = [descricao_to_tag(tuple(r.values())[0]) for r in remuneracao]
    values = [parse_ptbr_number(tuple(r.values())[1]) for r in remuneracao]
    return dict(zip(tags, values))
 
    
def get_full_link(link):
    """
    Add domain to link if missing:
    """
    domain = 'https://www.camara.leg.br'
    if link[4] != 'http' and link[0] == '/':
        return domain + link
    else:
        return link


def parse_remuneracao_info(soup, prefix='folha_'):
    """
    Extract information other than remuneração from
    a BeatifulSoup object `soup` built for a comissionado
    remuneração webpage, using hard-coded identifiers.
    
    Return a dict with information found, where the 
    keys get `prefix` (str).
    
    E.g.: 
    {'folha_categoria_funcional': ' SECRETÁRIO PARLAMENTAR',
     'folha_data_exercicio': ' 30/11/2017',
     'folha_cargo': ' Secretário Parlamentar',
     'folha_funcaoXcargo_comissao': ' SP13'}
    """
    
    # Find items containing information:
    extra_info = soup.find_all(attrs={'class': 'remuneracao-funcionario__info-item'})
    
    # Parse each item found into dict entry:
    info_dict  = {}
    for info in extra_info:
        key_value = info.text.split(':')
        info_dict[prefix + descricao_to_tag(key_value[0])] = key_value[1]
    
    return info_dict


def get_folha_remuneracao_data(comissionado_url):
    """
    Extract remuneração data from a remuneração webpage 
    of a Comissionado of a Deputado Federal.
    
    Input
    -----
    
    comissionado_url : str
        URL to the webpage contanining the comissionado's 
        remuneração. This URL is obtained from the table
        of comissionados of a Deputado, under the link 
        with text 'Consultar', after setting the month and 
        year of reference. 
        E.g.: https://www.camara.leg.br/transparencia/recursos-humanos/remuneracao/DEdGyYrBr6640vYO7egk?ano=2021&mes=3
        
    Return
    ------
    
    ref_date_n_type : list of str
        A list with two elements: a string representing the 
        month and year related to the remuneração, and a string
        specifying the kind of 'folha'.
    
    remuneracao_dict : dict
        A dict from the names of portions of remuneração
        in tag format (snake case, no accents) (str) to 
        their values (float).
    """

    # Start session:
    session = requests.Session()
    session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))

    # Use Beatiful soup to find salary table in a comissionado's page:
    response         = session.get(comissionado_url, timeout=5)
    comis_src        = BeautifulSoup(response.content, 'lxml')
    table_salary_raw = comis_src.find('table', attrs={'summary':None})

    # Extract salary information:
    if table_salary_raw != None:
        # Get table date and type:
        ref_date_n_type  = [s.strip() for s in comis_src.find('h3').text.split('–')]
        # Extract remuneração:
        table_salary     = html_table_to_dict_list(table_salary_raw)
        remuneracao_dict = structure_remuneracao(table_salary)
    # In case there is no salary information:
    else:
        ref_date_n_type  = [None] * 2
        remuneracao_dict = {}
    
    # Add extra information present in `comisisonado_url` webpage:
    remuneracao_dict.update(parse_remuneracao_info(comis_src))
        
    return ref_date_n_type, remuneracao_dict


def build_comissionado_remuneracao_dict(comissionado_data, comissionado_url, year, month):
    """
    Join basic information about comissionado, 
    extracted from the comissionado's table for 
    a deputado, with his/her remuneração.
    
    Input
    -----
    
    comissionado_data : dict
        Previouly collected data about a
        comissionado. This can be an empty
        dict {}.
    
    comissionado_url : str
        URL for the folha de remuneração 
        of comissionado, from where this 
        information will be extracted.
    
    year : int or str in format %Y
        The year for which the remuneração 
        will be collected.
    
    month : int or str in format %m
        The month for which the remuneração 
        will be collected.
        
    Return
    ------
    
    one_comissioado : dict
        The input information from 
        `comissionado_data` (with keys 
        formated as snake case, without
        accents) plus the remuneração 
        data.
    """
    
    # Get folha de remuneração:
    comissionado_url = '{}?ano={}&mes={}'.format(comissionado_url, year, month)
    ref_date_n_type, remuneracao_dict = get_folha_remuneracao_data(comissionado_url)

    # Join all information in the same dict:
    one_comissionado = {'comissionado_url': comissionado_url}
    one_comissionado.update({descricao_to_tag(k):v for k,v in comissionado_data.items()})
    one_comissionado.update(dict(zip(['data','tipo'], ref_date_n_type)))
    one_comissionado.update(remuneracao_dict)
    
    return one_comissionado


def extract_comissionados_data(response, year, month, drop_missing_remuneracao=False, verbose=False):
    """
    Given a GET HTTP request `response` for a 
    list of comissionados working for a deputado,
    return a list of dicts with the comissionados'
    info, contanining remuneração on `year` (int or 
    str) and `month` (int or str).
    
    The `response` is for URLs such as:
    https://www.camara.leg.br/deputados/137070/pessoal-gabinete?ano=2021
    
    If `drop_missing_remuneracao` (bool) is True, 
    do not return the comissionado dict if there 
    is no remuneração information about her/him 
    for the specified date.
    
    ATTENTION: When extracting comisisonado data from 
    past dates, the association to the deputado might 
    be wrong (a comissionado that was at a different gabinete
    will still have its remuneração to appear in the 
    same link associated to the current deputado).
    """
    
    # Use Beautiful soup to capture comissionado's list:
    if verbose: 
        print('Building soup...')
    soup   = BeautifulSoup(response.content, 'lxml')
    if verbose:
        print('Will look for table...')
    comis_table_raw = soup.find('table')

    # Exit if no table was found:
    if comis_table_raw == None:
        if verbose:
            print('No table was found.')
        return []

    # Parse table into dict (get names, cargos and start dates):
    if verbose:
        print('Will parse HTML table...')
    comis_table = html_table_to_dict_list(comis_table_raw)
    if verbose:
        print('# of entries in HTML table:', len(comis_table))
    
    # Get comissionado's salary links:
    if verbose:
        print('Will look for remuneração links...')
    links = comis_table_raw.find_all('a')
    comissionado_links = [get_full_link(link.attrs['href']) for link in links if link.text == 'Consultar']
    if verbose:
        print('# of links to remuneração:', len(comissionado_links))
    
    # Sanity check:
    assert len(comis_table) == len(comissionado_links), 'Found mismatched number of comissionados and links to remuneração.'
    
    # Collect remuneração information about all active comissionados (and format basic info):
    comis_data_list = []
    for comissionado_data, comissionado_url in zip(comis_table, comissionado_links):
        comis_data = build_comissionado_remuneracao_dict(comissionado_data, comissionado_url, year, month)
        if drop_missing_remuneracao == False or comis_data['data'] != None:
            comis_data_list.append(comis_data)
    
    return comis_data_list


def extract_year_month_from_key(key):
    """
    Given an AWS S3 `key` (str) for a file,
    extract and return the year (int) and
    month (int) specified in the key after
    'ano=' and 'mes='.
    """
    
    a_pos = key.find('ano=')
    year  = int(key[a_pos + 4:a_pos + 8])
    m_pos = key.find('mes=')
    month = int(key[m_pos + 4:m_pos + 5])
    
    return year, month


def entrypoint(response, event):
    """
    Capture and parse comissionados' data.
    
    Input
    -----
    
    response : http request get response
        The HTML page of the list of comissionados
        working in a gabinete.
    
    event : dict
        Something like
        {"aux_data": {},
        "bucket": "brutos-publicos",
        "data_path": ["dados"],
        "data_type": "external_module",
        "exclude_keys": None,
        "headers": {},
        "key": "legislativo/camara/scrapping/comissionados/camara-deputados-comissionados_id=137070&ano=2020&mes=6.json",
        "name": "camara-deputados-comissionados",
        "order": 0,
        "params": {},
        "records_keys": None,
        "url": "https://www.camara.leg.br/deputados/137070/pessoal-gabinete?ano=2020"}
        
    Return
    ------
    
    comis_data_list : list of dicts
        Each dict contains the data from a comissionado.
    """
    
    year, month = extract_year_month_from_key(event['key'])
    comis_data_list = extract_comissionados_data(response, year, month - 1)
    
    return comis_data_list