# Entrypoint for processing data according to dynamodb config "classify_reqs_camara".

import pandas as pd
import re


def sel_dou_1(input_data):
    """
    Use hard-coded criteria to pre-select articles from DOU section 1.
    Input and output are Pandas DataFrames.
    """
    identifica_regex = '(?:portaria|decreto|resolu|medida provisória)'
    veto_orgao_regex = '(?:universidade|instituto federal|superintendência regional|superintendência estadual|colégio|coordenação de processos migratórios|secretaria de fomento e incentivo à cultura|departamento de radiodifusão comercial)'
    veto_orgao_root  = ['Conselho Nacional do Ministério Público',
                        'Entidades de Fiscalização do Exercício das Profissões Liberais', 
                        'Governo do Estado', 'Ineditoriais', 'Defensoria Pública da União', 
                        'Ministério Público da União', 'Poder Judiciário', 'Prefeituras', 
                        'Tribunal de Contas da União', 'Atos do Poder Judiciário']

    # Get secao 1:
    sel_data = input_data.loc[input_data['secao'] == 1]
    
    # Apply cuts:
    sel_data = sel_data.loc[(~sel_data['identifica'].isnull()) & 
                            (sel_data['identifica'].str.lower().str.contains(identifica_regex))]
    sel_data = sel_data.loc[~sel_data['orgao'].str.lower().str.contains(veto_orgao_regex)]
    sel_data = sel_data.loc[~sel_data.orgao.apply(lambda s: s.split('/')[0]).isin(veto_orgao_root)]
    
    return sel_data


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


def process(code, input_data):
    """
    Process `input_data` (Pandas DataFrame) using hard-coded instructions below
    and the python object `code`.
    """
    
    # Clean text:
    input_data['fulltext']    = input_data.apply(lambda r: clean_text(r['identifica'], r['ementa'], r['fulltext']), axis=1)
    input_data['resumo']      = input_data['fulltext'].apply(create_resumo)
    input_data['secao']       = input_data['secao'].apply(get_secao)
    input_data['data_pub']    = pd.to_datetime(input_data['data_pub'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
    input_data['tipo_edicao'] = input_data['edicao'].astype(str).apply(tipo_edicao_Q)

    # Select relevant data:
    input_data = sel_dou_1(input_data)
    
    # Predict:
    predicted_class = code.predict(input_data)

    # Join prediction to data:
    input_data['predicted_rank'] = pd.Series(predicted_class, index=input_data.index)
    
    return input_data
