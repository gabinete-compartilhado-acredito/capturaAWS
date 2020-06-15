import requests
from lxml import html
import json
from datetime import datetime
from collections import defaultdict
import re

def select_article(response):
    """
    Transforms html into lxml data type and selects only 
    article div
    
    input: 
        response: requests.models.Response
    return: lxml.html.HtmlElement
    """
    tree  = html.fromstring(response.text) # Changed from .content to .text to avoid decoding errors later on (2020-04-15)
    xpath = '//*[@id="materia"]'
    return tree.xpath(xpath)[0]

def recurse_over_nodes(tree, parent_key, data):
    """
    Recursevely gets the text of the html leafs and saves
    its classes and keys and text as values
    
    input: 
        tree: lxml.html.HtmlElement
        parent_key: lxml.html.HtmlElement
        data: dict
    return: dict
    """
    
    def add_to_data(data, key):
        
        if key in data:
            if data[key] is None:
                data[key] = branch.text
            else:
                data[key] = data[key] + ' | %s' % branch.text
            
        else:
            data[key] = branch.text
        
        return data
        
    
    for branch in tree:
        key = '-'.join(list(branch.classes))
        
        if branch.getchildren():
            
            if parent_key:
                key = '%s_%s' % (parent_key, key)
                
            add_to_data(data, key)
            
            data = recurse_over_nodes(branch, key, data)
        
        else:
            
            if parent_key:
                key = '%s_%s' % (parent_key, key)
            
            add_to_data(data, key)
    
    return data

def decode(data, encoding= 'iso-8859-1', decoding='utf8'):
    """
    Change enconding from string with secure error handling
    
    input:
        data: dict
        encoding: string
        decoding: string
    return: dict
    """
    
    final = {}
    
    for k, v in data.items():
        
        try:
            final[k] = v.encode('iso-8859-1').decode('utf8')
        except Exception as e:
            print("Error", e)
            final[k] = v
    
    return final

def filter_keys(data):
    """
    Filter keys paths to get only last class from html
    
    input:
        data: dict
    return: dict
    """

    final = defaultdict(lambda: '')

    for k, v in data.items():

        if v is not None:
            
            k_new = k.split('_')[-1]

            final[k_new] =  ' | '.join([final[k_new], v]) if len(final[k_new]) > 0 else v
            
    return final

def filter_values(data):
    """
    Filter values that do not have letters or numbers
    
    input:
        data: dict
    return: dict
    """
    
    final = {}
    
    for k, v in data.items():
        
        if re.search('[a-zA-Z0-9]', v):
        
            final[k] = v
            
    return final


def decoded_full_text(article):
    """
    Get articles' full text (without identifying html tags).
    """
    try:
        full_text = html.tostring(article, method='html', encoding='utf-8').decode('utf-8')
        full_text = re.sub('<.+?>', ' ', full_text)
        full_text = ' '.join(full_text.split())
        full_text = re.sub('<.+?>', ' ', full_text)
    except UnicodeEncodeError:
        full_text = html.tostring(article, method='html', encoding='iso-8859-1').decode('utf-8')
        full_text = re.sub('<.+?>', ' ', full_text)
        full_text = ' '.join(full_text.split())
        full_text = re.sub('<.+?>', ' ', full_text)
    except UnicodeDecodeError:
        full_text = html.tostring(article, method='html', encoding='utf-8').decode('utf-8')
        full_text = re.sub('<.+?>', ' ', full_text)
        full_text = ' '.join(full_text.split())
        full_text = re.sub('<.+?>', ' ', full_text)
    except:
        full_text = None
        
    return full_text


def make_resumo(fulltext):
    """
    Given a string (fulltext), this function aims to extract 
    the most important part of it as a abstract.
    """

    # Termos a serem pesquisados:
    termos = ['resolve:', 'onde se l', 'objeto:', 'espécie']
    # Tamanho do resumo:
    resumo_size = 300
    
    # Alterando o texto para minúsculo    
    fulltext  = str(fulltext)
    paragraph = fulltext.lower()
         
    for termo in termos:
        
        pos = paragraph.find(termo)
        
        if pos != -1: 
            # Se encontra algum dos termos, resume o texto com os 300 primeiros caracteres 
            # a partir do termo encontrado.
            abstract = fulltext[pos:pos + resumo_size]    
            break            # O break aqui serve para garantir que, caso um termo seja encontrado, 
                             # não busque pelos demais.
        
    if pos == -1:
            abstract = fulltext[:resumo_size]   # Se não encontra nenhum dos termos, resume o texto 
                                                # nos primeros 300 caracteres.          
    
    if len(fulltext[pos:]) > len(abstract):
        abstract = abstract + '...'
    
    return abstract


def get_data(article):
    """
    Get relevant data from html. It recursevely gets leaf text from html
    and saves theirs classes as keys. 
    It also creates an item in dict's key 'full-text' with all text 
    in the html, without tags.
    
    input: 
        article: lxml.html.HtmlElement
    return: dict
    """
    data = recurse_over_nodes(article, None, {})

    # filtra None e melhora keys
    data = filter_keys(data)
    data = filter_values(data)
    data = {k: v for k,v in data.items() if len(k) != 0}

    # encoding para utf-8
    #data = decode(data)
    
    # Include full-text:
    data['fulltext'] = decoded_full_text(article)
    # Include resumo:
    data['resumo'] = make_resumo(data['fulltext'])
    
    return data


def get_url_certificado(article):
    """
    Gets in certified url in the html
    
    input: 
        artigo: lxml.html.HtmlElement
    return: string
    """

    return article.xpath('//*[@class="botao-materia"]/a[@href]/@href')[0]

def data_schema(key, value, url, url_certificado):
    """
    Final data schema
    
    input:
        key: string
        value: string
        url: string
        url_certificado: string
    return: dict
    """
    
    return {
        "key": key,
        "value": value,
        "url": url,
        "capture_date": datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
        "url_certificado": url_certificado
    }

def structure_data(data, url, article):
    """
    Structures html parsed data to list of dicts 
    ready to be processed by http-request Lambda
    Function.
    It adds the capture date, url, and 
    certified url
    
    input: 
        data: dict
        url: string
        artigo: lxml.html.HtmlElement
    return: list of dict
    """
    
    url_certificado = get_url_certificado(article)
    
    final = []
    for key, value in data.items():
        
        final.append(data_schema(key, value, url, url_certificado))
        
    return final
        

def entrypoint(response, event):

    url = event['url']

    article = select_article(response)
    
    data = get_data(article)
    
    data = structure_data(data, url, article)
    
    return data