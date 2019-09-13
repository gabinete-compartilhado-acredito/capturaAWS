import json
import boto3
import google.auth
from google.cloud import bigquery
from collections import defaultdict

# Switch for turn on debugging messages.
debug = False

s3 = boto3.client('s3')
a = s3.get_object(
                  Bucket='config-lambda', 
                  Key='layers/google-cloud-storage/gabinete-compartilhado.json')
open('/tmp/key.json', 'w').write(a['Body'].read().decode('utf-8'))

# Create credentials with Drive & BigQuery API scopes
# Both APIs must be enabled for your project before running this code
credentials, project = google.auth.default(scopes=[
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery',
])
bq = bigquery.Client(credentials=credentials, project=project)


def query_bigquery(query):
        
    result = bq.query(
        query,
        # Location must match that of the dataset(s) referenced in the query.
        location="US",
    )  # API request - starts the query
    
    result = [dict(r.items()) for r in result] 
    
    return result

    
def translate_to_slack(datum, casa):
    """
    For a single entry in the query results, create a dict with the relevant information.
    """
    
    if casa == 'camara':
        
        datum['url_proposicao'] = 'http://www.camara.leg.br/proposicoesWeb/fichadetramitacao?idProposicao=' + str(datum['id'])
    
    elif casa == 'senado':
        
        datum['url_proposicao'] = 'https://www25.senado.leg.br/web/atividade/materias/-/materia/' + str(datum['id'])
        
    if casa == 'camara' or casa == 'senado':
        return dict(
            url_proposicao=   datum['url_proposicao'],
            tipo=             datum['sigla_tipo'],
            numero=           datum['numero'],
            ano=              datum['ano'],
            autor=            datum['nome_autor'],
            partido_autor=    datum['sigla_partido_autor'],
            relator=          datum['relator'],
            partido_relator=  datum['partido_relator'],
            ementa=           datum['ementa'],
            url_inteiro_teor= datum['url'],
            status=           datum['descricao_tramitacao'],
            sequencia=        datum['sequencia'],
            regime=           datum['regime'],
            situacao=         datum['decricao_situacao'],
            orgao=            datum['sigla_orgao'],
            despacho=         datum['despacho'])
    elif casa == 'dou':
        if datum['ementa'] == 'None':
            resumo_tipo0 = 'Excerto'
            resumo0 = datum['resumo']
        else:
            resumo_tipo0 = 'Ementa'
            resumo0 = datum['ementa']
        return dict(
            identifica=      datum['identifica'],
            orgao=           datum['orgao'],
            assina=          datum['assina'],
            cargo=           datum['cargo'],
            pub_date=        datum['pub_date'],
            edicao=          datum['edicao'],
            secao=           datum['secao'],
            pagina=          datum['pagina'],
            url=             datum['url'],
            url_certificado= datum['url_certificado'],
            resumo_tipo=     resumo_tipo0,
            resumo=          resumo0)
    
    else:
        return datum

def to_slack_format(bot_info, result):
    """
    Takes the bigQuery result and transforms it into a new structure that
    contains metadata about the media destination.
    """
    print('first result', len(result))
    
    # Remove double quotes from bigQuery results (list of dicts which are new tramitações)
    # to avoid bugs:
    result = map(lambda x: {k: str(v).replace('"', '') for k, v in x.items()}, result)
    
    # Add default dict to avoid error if chosen key is inexistent 
    # (in this case, return ''):
    result = list(map(lambda x: defaultdict(lambda: '', x), result))
    
    # Reorganize and rename dict:
    result = map(lambda x: translate_to_slack(x, bot_info['casa']), result)
    
    # Create a dict with metadata and the data as a list inside a key:
    payload = dict(
        casa = bot_info['casa'],
        descricao_post= bot_info['media']['description'],
        media={'type': bot_info['media']['type'],
                'channel': bot_info['media']['channel']},
        data=list(result))
    
    # Create a json from the structure above:
    return json.dumps(payload, ensure_ascii=False)

def post_to_sns(bot_info, payload):
    
    print('Sending to SNS')
    sns = boto3.client('sns')
    # Create a topic in the SNS if it does not exist:
    res = sns.create_topic(Name=bot_info['sns-topic'])
    # Subscribe the Lambda function to the topic created above:
    sns.subscribe(TopicArn=res['TopicArn'],
                  Protocol='lambda',
                  Endpoint='arn:aws:lambda:us-east-1:085250262607:function:post-to-slack:JustLambda')
    
    # Send the payload to the topic. Everyone subscribed (in this case, the Lambda 
    # function 'post-to-slack') will receive the Message:
    sns.publish(TopicArn=res['TopicArn'],
                Message=payload)
    
def get_relevant_results(bot_info, results):
    """
    Filter the query result 'results' that gets the last 30 minutes new stuff
    in the database.
    -- results: list of dicionaries. Each entry in list (a dict) is a row in the query results,
       and each key in dict is a column.
       
    # About bot_info['filters']:
    # This is a list of filters (dict), each filter has the entries:
    # 1 FILTER: column_name, positive_filter, negative_filter.
    #           -- The value for column_name is the column to check.
    #           -- The positive and negative_filter are a list of keywords that are combined with OR;
    #           -- The positive and negative filters are combined with AND;
    # The filters are combined with AND;
    # To combine filters with OR, create a new bot_info with a different list of filters.
    """
    
    filters = bot_info['filters']
    
    if len(filters):
        print('name:', bot_info['nome'])
        print('len results', len(results))

        
        for f in filters:
           
            if debug:
                print('f', f)
            
            # Delete None
            # Select all desired columns that actually exist in the bigquery results:
            results = list(filter(lambda x: x[f['column_name']] is not None, results))
                
            if 'positive_filter' in f.keys():
                results = list(filter(lambda x: 
                    any([var.lower() in x[f['column_name']].lower() 
                        for var in f['positive_filter']]), results))
                        
            # Changed on 2019-05-21 from elif to if:
            if 'negative_filter' in f.keys():
                results = list(filter(lambda x: 
                    all([var.lower() not in x[f['column_name']].lower() 
                        for var in f['negative_filter']]), results))
                        
            print('len results', len(results))
    
    return results
    
def data_to_sns(bot_info, results):
    """
    Takes the dicionary with search keywords 'bot_info' and all new data in BigQuery
    'results', selects the relevant entries, format to slack style and send
    it to Amazon's SNS.
    """
    # Filter the results (select what we want to track):
    partial_results = get_relevant_results(bot_info, results)
    # Format to slack structure:
    payload = to_slack_format(bot_info, partial_results)
    # Send the post to Amazon's SNS (notification system):
    post_to_sns(bot_info, payload)

# Functions to load Gabi filters:

def csvrow_to_list(csvrow):
    """
    Takes a string 'csvrow' that has substrings separated by commas and 
    returns a list of substrings.
    """
    if csvrow != None:
        return list(map(lambda s: s.strip(), csvrow.split(';')))
    else:
        return None

def check_for_ambiguity(filter_group, filter_ids, key):
    """
    Check if all filters numbered the same way have the same tag 'key'
    (nome, casa, channel, description). Returns an error if it 
    doesn't. This is needed to group filters under the same 
    filter set.
    -- filter_group: pandas grouby object, grouped by filter number.
    -- key: nome, casa, channel, description
    """
    unique_key = all([[f[key] == filter_group[i][0][key] for f in filter_group[i]] for i in filter_ids])
    if not unique_key:
        raise Exception('Found multiple entries of \''+key+'\' for same filter.')

def get_filter_par(filter_group, filter_ids, key):
    """
    Given a pandas groupby object 'filter_group' that group the 
    filters by filter_number (giving a filter set) and a key, 
    get the first value for that key in each group.
    """
    return [filter_group[i][0][key] for i in filter_ids]

def filterset_gen(filters_raw, fnumber):
    """
    Group all filters in table 'filter_raw' in a filter set according to filter_number 'fnumber'.
    Then, transforms comma separated filter keywords into a list.
    It also deals with missing values in filter entries / filter entry at all.
    """
    # If there is no filter, return an empty list:
    flist = [f for f in filters_raw if f['filter_number'] == fnumber]
    if len(flist) == 1 and flist[0]['column_name'] == None:
            return []
    else:
        # If there are filters, group them:
        return [{k:(f[k] if k=='column_name' else csvrow_to_list(f[k])) 
                 for k in ('column_name', 'positive_filter', 'negative_filter') if f[k]!=None} 
                for f in filters_raw if f['filter_number'] == fnumber]
    
def format_filters(filters_raw):
    """
    Format the filters loaded from Google sheets (a table 'filters_raw')
    into the dict used by João Carabetta:
    """
    # Group list of filters by filter_number, to form a filter set:
    filter_group = defaultdict(list)
    for f in filters_raw:
        filter_group[f['filter_number']].append(f)
    # Get filter set ids:
    filter_ids = list(filter_group.keys())
    # List of filter set tags:
    filter_id_keys = ['nome', 'casa', 'channel', 'description']

    # Check if every filter set has unique tags:
    dump = [check_for_ambiguity(filter_group, filter_ids, key) for key in filter_id_keys]
    # Get filter set tags:
    filter_tag = {key: get_filter_par(filter_group, filter_ids, key) for key in filter_id_keys}

    # Organize filters in a filter set:
    filter_set = [filterset_gen(filters_raw, fnumber) for fnumber in filter_ids]

    # Put all filter sets (with tags) into a list:
    event = [{'nome': filter_tag['nome'][i], 
              'casa': filter_tag['casa'][i],
              'media': {'type': 'slack', 
                        'channel': filter_tag['channel'][i],
                        'description': filter_tag['description'][i]},
              #'sns-topic': 'slack-test-DEV', # For debugging.
              'sns-topic': 'slack-test',
              'filters': filter_set[i]} 
             for i in range(len(filter_ids))]
    
    return event
    
def lambda_handler(event, context):
    
    # Load filters information from Google Sheets:
    #filters_raw = query_bigquery('SELECT * FROM `gabinete-compartilhado.gabi_bot.gabi_filters`')
    filters_raw = query_bigquery("SELECT * FROM `gabinete-compartilhado.gabi_bot.gabi_filters` WHERE casa != 'dou'")
    # Each entry in the list below is a bot_info:
    event = format_filters(filters_raw)

    # Will get every new entry in BigQuery that appeared in the last 30min:
    queries_metadata = [
        {'casa': 'camara',
        'query': "SELECT * FROM `gabinete-compartilhado.gabi_bot.camara_tramitacao_last30minutos`"},
        #'query': "SELECT * FROM `gabinete-compartilhado.gabi_bot.teste`"},
        {'casa': 'senado',
        'query': "SELECT * FROM `gabinete-compartilhado.gabi_bot.senado_tramitacao_last30minutos`"}#,
        #{'casa': 'dou',
        #'query': "SELECT * FROM `gabinete-compartilhado.gabi_bot.artigos_dou_last30minutos`"}
        ]
    
    for metadata in queries_metadata:
        metadata['results'] = query_bigquery(metadata['query'])
    
    # LOOP over the different selection criteria:
    for bot_info in event:
        
        if 'dou' in bot_info['casa']:
            data_to_sns(bot_info, queries_metadata[2]['results'])
            
        elif 'senado' in bot_info['casa']:
            data_to_sns(bot_info, queries_metadata[1]['results'])

        elif 'camara' in bot_info['casa']:
            data_to_sns(bot_info, queries_metadata[0]['results'])