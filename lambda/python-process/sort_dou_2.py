# Entrypoint for processing data according to dynamodb config "sort_dou_2".

import pandas as pd
import re
import dou_preparer as dp


def sel_dou_2(input_data):
    """
    Use hard-coded criteria to pre-select articles from DOU section 2.
    Input and output are Pandas DataFrames.
    """
    
    veto_orgao_regex = '(?:universidade|instituto federal|superintendência regional|gerência regional|superintendência estadual|colégio|comando militar|hospital)'
    veto_orgao_root  = ['Conselho Nacional do Ministério Público',
                        'Entidades de Fiscalização do Exercício das Profissões Liberais', 
                        'Governo do Estado', 'Ineditoriais', 'Defensoria Pública da União', 
                        'Ministério Público da União', 'Poder Judiciário', 'Prefeituras', 
                        'Tribunal de Contas da União', 'Atos do Poder Judiciário', 'Poder Legislativo']
    veto_fulltext_regex = '(?:conceder aposentadoria|aposentar|conceder pensão|afastamento do país)'
    
    # Get secao 2:
    sel_data = input_data.loc[input_data['secao'] == 2]
    
    # Apply cuts:
    sel_data = sel_data.loc[~sel_data['orgao'].str.lower().str.contains(veto_orgao_regex)]
    sel_data = sel_data.loc[~sel_data.orgao.apply(lambda s: s.split('/')[0]).isin(veto_orgao_root)]
    sel_data = sel_data.loc[~sel_data['fulltext'].str.lower().str.contains(veto_fulltext_regex)]
    
    return sel_data
    

def process(code, input_data):
    """
    Process `input_data` (Pandas DataFrame) using hard-coded instructions below
    and the python object `code`.
    """
    
    # Pre-process DOU data in place, to reproduce transformations made with SQL in the 
    # model training data.
    dp.prepare_dou_df(input_data)

    # Select relevant data:
    input_data = sel_dou_2(input_data)
    
    # Predict:
    predicted_class = code.predict(input_data)

    # Join prediction to data:
    input_data['predicted_rank'] = pd.Series(predicted_class, index=input_data.index)
    
    return input_data
