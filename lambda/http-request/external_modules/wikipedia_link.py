from bs4 import BeautifulSoup
import requests

def get_google_1st_result(query):
    """
    Return the url of the first result in a Google search 
    `query`.
    """
    
    # Get google page:
    url = 'http://www.google.com'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')

    # Find google query form:
    form = soup.find('form')
    executor = form.attrs['action']
    inputs   = form.find_all('input')
    
    # Run query:
    response = requests.get(url+executor, params={'q': query, 'btnI':'Estou com sorte', 'hl':'en-US'})
    # Get first link:

    soup = BeautifulSoup(response.text, 'lxml')
    first_link = soup.find('a').attrs['href']
    
    return first_link

def get_wikipedia_intro(titulo, nome):
    """
    Get the first link returned by a Google search
    in pt.wikipedia for `titulo` (str) followed by `nome` (str).
    Return a list of dicts.
    """
    query = '{} {} site:pt.wikipedia.org'.format(titulo, nome)
    url = get_google_1st_result(query)
    
    # Return None if page not found:
    root = 'https://pt.wikipedia.org/'
    if url[:len(root)] != root:
        return None

    return url
    

def get_first_paragraphs(url):
    """
    Request link `url` (str) and return the first paragraph or so,
    limited to the hard-coded number of characters.
    """
    # Hard-coded:
    max_chars = 500
    
    # Get wikipedia article:
    response = requests.get(url)

    # Get paragraphs:
    soup = BeautifulSoup(response.text, 'lxml')
    paragraphs = soup.find_all('p')
    
    # Concatena parágrafos até o máximo de caracteres:
    intro = ''
    for paragraph in paragraphs:
        intro = intro + paragraph.text
        if len(intro) > max_chars:
            break
    
    return intro
    

def wikipedia_info(response, event):
    
    url   = get_wikipedia_intro(event['aux_data']['titulo'], event['identifier'])
    intro = get_first_paragraphs(url) 
    
    return [{'titulo': event['aux_data']['titulo'], 'nome': event['identifier'], 'first_paragraphs': intro, 'wiki_link': url}]
