# These functions take raw DOU data and process it like it was processed in BigQuery SQL.
# The thing is the data used to train the model was preprocessed by SQL, so we had to 
# reproduce the steps here. Not the best approach.

import re
import pandas as pd

def clean_text(identifica, ementa, fulltext):
    """
    Given a DOU article titles `identifica` and an abstract `ementa`, 
    remove the first title in `identifica`, `ementa` and the hard-coded
    footnote from the full text of the DOU article `fulltext`.
    """
    # ATTENTION: this code should reproduce the cleaning performed in BigQuery,
    # since its resulting text was used to train the model.
    
    if identifica == None:
        return fulltext

    if fulltext == None:
        return None
    
    # Remove primeiro título que aparece no artigo (e tudo que vem antes):
    first_identifica = identifica.split(' | ')[0]
    text_pos   = fulltext.find(first_identifica) + len(first_identifica)
    clean_text = fulltext[text_pos:]
    
    # Remove rodapé:
    clean_text = clean_text.replace('Este conteúdo não substitui o publicado na versão certificada.', '')
    
    if ementa == None:
        return clean_text
    
    # Remove ementa:
    clean_text = clean_text.replace(ementa, '')
    
    return clean_text    


def create_resumo(fulltext):
    """
    Get the first `resumo_length` (hard-coded) characters from `fulltext` 
    that appear after `beginning_marker` (hard-coded) or small variations 
    of it. If `beginning_marker` is not found, return `fulltext`.
    """
    beginning_marker = 'resolve'
    resumo_length    = 500

    if fulltext == None:
        return None
    
    marker_pos = fulltext.find(beginning_marker)
    if marker_pos != -1:
        marker_pos = marker_pos + len('resolve')
        
        if fulltext[marker_pos] == 'u':
            marker_pos = marker_pos + 1
        if fulltext[marker_pos] == ':':
            marker_pos = marker_pos + 1
        return fulltext[marker_pos: marker_pos + resumo_length].strip()
    
    return fulltext[:resumo_length].strip()


def get_secao(secao_dou):
    """
    Extract the first single digit in `secao_dou` (str) and
    return it as an int. The purpose is to parse the DOU section.
    """
    match = re.search('[0-9]', secao_dou)
    
    if type(match) != type(None):
        return int(match[0])
    
    return match


def tipo_edicao_Q(edicao):
    """
    Define se edição do artigo é ordinária ou extra.
    """
    return 'Extra' if len(edicao.split('-')) > 1 else 'Ordinária'


def prepare_dou_df(input_data):
    """
    Transforms a Pandas DataFrame with DOU articles' data in place.
    """
    # Clean text:
    input_data['fulltext']    = input_data.apply(lambda r: clean_text(r['identifica'], r['ementa'], r['fulltext']), axis=1)
    input_data['resumo']      = input_data['fulltext'].apply(create_resumo)
    input_data['secao']       = input_data['secao'].apply(get_secao)
    input_data['data_pub']    = pd.to_datetime(input_data['data_pub'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
    input_data['tipo_edicao'] = input_data['edicao'].astype(str).apply(tipo_edicao_Q)
