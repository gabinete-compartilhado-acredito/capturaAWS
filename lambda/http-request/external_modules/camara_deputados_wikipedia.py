import wikipedia_link as wl

def entrypoint(response, event):
    """
    Gets the wikipedia (PT) link to a certain parlamentar.
    
    Input
    -----
    
    event : dict
        A dict containing all the required info to get the tweets.
        We need 'titulo' ('deputado' or 'senador') and 
        'nome'.
        
    response : anything
        This particular module does not use `response`.
        
    Returns
    -------
    
    wiki_link : list of dicts
        List with one dict: the titulo, nome and wikipedia link.
    """
    
    return wl.wikipedia_info(response, event)