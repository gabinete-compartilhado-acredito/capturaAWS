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
    

def entrypoint(response, event):
    
    url = event['url']
    
    # Use Beautiful soup to capture comissionado's list:
    soup   = BeautifulSoup(response.content, 'lxml')
    comis_table_raw = soup.find('table')
    # Exit if no table was found:
    if comis_table_raw == None:
        return []
    
    # Parse table into dict:
    comis_table = html_table_to_dict_list(comis_table_raw)
    
    # Get comissionado's salary links:
    links = soup.find_all('a')
    comissionado_links = [link.attrs['href'] for link in links if link.text == 'Consulta']
    
    # Collect information about all active comissionados:
    comis_data_list = []
    session = requests.Session()
    session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))
    for i in range(len(comis_table)):
        
        # Use Beatiful soup to find salary table in a comissionado's page:
        response         = session.get(comissionado_links[i], timeout=5)
        comis_src        = BeautifulSoup(response.content, 'lxml')
        table_salary_raw = comis_src.find('table', attrs={'summary':None})
        
        # Extract salary information:
        if table_salary_raw != None:
            ref_date_n_type  = [s.strip() for s in table_salary_raw.find('caption').text.split('\n')[1].split('-')]
            table_salary     = html_table_to_dict_list(table_salary_raw)
            remuneracao_dict = structure_remuneracao(table_salary)
        # In case there is no salary information:
        else:
            ref_date_n_type  = [None] * 2
            remuneracao_dict = {}

        # Join all information in the same dict:
        one_comissionado = {'comissionado_url': response.url}
        one_comissionado.update({descricao_to_tag(k):v for k,v in comis_table[i].items()})
        one_comissionado.update(dict(zip(['data','tipo'], ref_date_n_type)))
        one_comissionado.update(remuneracao_dict)

        comis_data_list.append(one_comissionado)
    
    return comis_data_list