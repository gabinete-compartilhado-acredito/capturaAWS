from lxml import etree
from datetime import datetime
from collections import defaultdict
import io
import zipfile
import re

def load_zipped_response(response):
    """
    Download a ZIP file and extract its contents in memory
    yields (filename, file-like object) pairs
    """

    # Extract the contents of the .zip file in memory
    zip_file = io.BytesIO(response.content)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for xml_file in zip_ref.namelist():
            with zip_ref.open(xml_file) as f:
                # Read the xml file from memory
                xml_data = f.read()
            # Parse the XML data
            root = etree.fromstring(xml_data)
            yield root, xml_file


def branch_text(branch):
    """
    Takes and lxml tree element 'branch' and returns its text, 
    joined to its children's tails (i.e. the content that follows 
    childrens of 'branch').
    """
    texts = list(filter(lambda s: s != None, [branch.text] + [child.tail for child in branch]))
    if len(texts) == 0:
        return None
    text  = ' | '.join(texts)
    return text


def add_to_data(branch, data, key):
    """
    Given a dict 'data' and a key, add the text (see branch_text function) 
    found in a branch to its value.
    """
    if key in data:
        if data[key] is None:
            data[key] = branch_text(branch)
        else:
            data[key] = data[key] + ' | %s' % branch_text(branch)            
    else:
        data[key] = branch_text(branch)        
    return data


def recurse_over_nodes(tree, parent_key, data):
    """
    Recursevely gets the text of the xml leafs and saves
    its classes and keys and text as values
    
    input: 
        tree: lxml.etree._Element
        parent_key: lxml.etree._Element
        data: dict
    return: dict
    """            
    for branch in tree:
        try:
            key = branch.attrib.get('class')
        except:
            key = branch.attrib.get('id')
        
        if list(branch):
            if parent_key:
                key = '%s_%s' % (parent_key, key)    
            add_to_data(branch, data, key) 
            data = recurse_over_nodes(branch, key, data)
        
        else:            
            if parent_key:
                key = '%s_%s' % (parent_key, key)            
            add_to_data(branch, data, key)
    
    return data

def parse_xml_flattened(element, parent_key=""):
    result = {}
    for child in element:
        key = parent_key + child.tag
        if len(child) > 0:
            result.update(parse_xml_flattened(child, key + "-"))
        else:
            result[key] = child.text
    return result


def extract_necessary_fields(article):
    """ 
    Extract necessary fields that were present in html parser but
    are not present as keys in inlabs xml response
    """
    orgao_dou_data = article.xpath('//article/@artCategory')[0]
    edicao_dou_data =  article.xpath('//article/@editionNumber')[0]
    secao_dou_data = article.xpath('//article/@numberPage')[0]
    secao_dou = article.xpath('//article/@pubName')[0]
    publicado_dou_data = article.xpath('//article/@pubDate')[0]

    # Compile a regular expression pattern to search for the "assina" class
    pattern = re.compile(r'<p class="assina">(.*?)</p>')
    # Search for the pattern in the assina variable
    match = pattern.search(article.xpath('.//article/body/Texto/text()')[0])
    # Extract the text within the <p> tags
    if match:
        assina_text = match.group(1)
        assina = assina_text
    else:
        assina = ''
        
    return {'orgao-dou-data': orgao_dou_data, 'edicao-dou-data': edicao_dou_data, 
            'secao-dou-data': secao_dou_data, 'assina': assina, 'secao-dou': secao_dou,
            'publicado-dou-data': publicado_dou_data}

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

def get_url_certificado(article):
    """
    Gets the certified url in the xml
    
    input: 
        article: lxml.etree._Element
    return: string
    """
    return article.xpath('//article/@pdfPage')[0]

def get_data(article):
    """
    Get relevant data from xml. It recursevely gets leaf text from xml
    and saves theirs classes as keys. 
    It also creates an item in dict's key 'full-text' with all text 
    in the xml, without tags.
    
    input: 
        article: lxml.etree._Element
    return: dict
    """
    data = parse_xml_flattened(article)

    # filtra None e melhora keys
    data = filter_keys(data)
    data = filter_values(data)
    data = {k: v for k,v in data.items() if len(k) != 0}

    # Include other fields from extraction
    fields = extract_necessary_fields(article)
    data['orgao-dou-data'] = fields['orgao-dou-data']
    data['edicao-dou-data'] = fields['edicao-dou-data']
    data['secao-dou-data'] = fields['secao-dou-data']
    data['assina'] = fields['assina']
    data['secao-dou'] = fields['secao-dou']
    data['publicado-dou-data'] = fields['publicado-dou-data']

    # Include full-text ignoring xml tags and formatters:
    full_text = etree.tostring(article, pretty_print=True, encoding='unicode')
    full_text = ' '.join(full_text.split())
    full_text = re.search(r'<Texto>(.*?)</Texto>', full_text).group(1)
    full_text = full_text.replace('&lt;','<').replace('&gt;','>')
    clean_full_text = re.sub(r'<[^>]+>', '', full_text)

    data['fulltext'] = clean_full_text

    return data

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
        

def parse_dou_article(response, url=''):
    """
    Gets an HTTP request response for a DOU article's URL and that url 
    and parse the relevant fields to a list of dicts. Each dict has the 
    keys: 
    * key             -- an html tag class identifying the field;
    * value           -- the respective value (text) in that field;
    * url             -- The original article URL;
    * capture_date    -- The date when capture occured;
    * url_certificado -- The link to the certified version of the article.
    """
    data    = get_data(response)    
    data    = structure_data(data, url, response)
    
    return data