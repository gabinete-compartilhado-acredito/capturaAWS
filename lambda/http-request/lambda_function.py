import json
import requests
import boto3
from botocore.exceptions import EndpointConnectionError
import xmltodict
from collections import MutableMapping
from datetime import datetime
from dynamodb_json import json_util as dyjson 
import re
import sys
sys.path.insert(0, "external_modules")
import importlib

# For debugging (print out more comments during execution):
debug = False
# To run it locally (not in AWS), set to True:
local = False


def get_nested_dict(data, keys):

    for key in keys:

        data = data[key]
    
    return data
    
    
def add_url_and_capture_date(in_json, event):
    
    # add url to all records
    if not isinstance(in_json, list):
        in_json = [in_json]
        
    for record in in_json:
        if 'url' in event:
            api_url = event['url']
        else:
            api_url = None
        record.update({
            'api_url': api_url,
            'capture_date': datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        })
    
    return in_json


def delete_keys_from_dict(dictionary, event):

    def internal_func(dictionary, keys):

        modified_dict = {}
        for key, value in dictionary.items():
            if key not in keys_set:
                if isinstance(value, MutableMapping):
                    modified_dict[key] = internal_func(value, keys_set)
                else:
                    modified_dict[key] = value  # or copy.deepcopy(value) if a copy is desired for non-dicts.
        return modified_dict
        
    keys = event.get('exclude_keys')
    
    if keys is None:
        return dictionary
    
    keys_set = set(keys)  
    
    return internal_func(dictionary, keys_set)


def preserve_root_data(dictionaries, event):
    
    def internal_func(dictionary, event):
        
        # Get records in dictionary
        records = dictionary
        for key in records_keys:
            if records is None:
                return False
            records = records.get(key)
        
        # Updates the records with root data, similar to flattening
        root_keys = [r for r in list(dictionary.keys()) if r not in records_keys]
    
        for record in records:
        
           record.update({key: dictionary[key] for key in root_keys})
        
        return records
        
    # Get the keys that point to relevant data, return the dict otherwise
    records_keys = event.get('records_keys')
    
    if records_keys is None:
        return dictionaries, True
    
    dictionaries = [internal_func(dictionary, event) for dictionary in dictionaries]    
    
    if any(dictionaries) == False:
        
        return None, False
    
    flatten = lambda l: [item for sublist in l for item in sublist]
  
    return flatten(dictionaries), True


def postprocessor(path, key, value):
    
    return re.sub(r'[\W_]+', u'', key, flags=re.UNICODE), value


def load_as_json(event, response):
    """
    Se esperamos um json na response (a espera é especificada 
    no event), retorna o json; se for xml, converte para dicionário.
    """
    
    if event['data_type'] == 'json':
        data = response.json()

    elif event['data_type'] == 'xml':
        data = xmltodict.parse(response.text, postprocessor=postprocessor)
    # Resposta padrão: dicionário vazio.
    else:
        data = {}
    
    return data


def filter_data(event, data):
    """
    Pega configurações descritas no event e dados baixados e 
    seleciona os dados que desejamos e salva como uma lista de dicionários 
    no in_json. save_to_s3 retorna um status.
    """
    
    # Seleciona o nível do json que queremos:
    in_json = get_nested_dict(data, event['data_path'])
    
    # Do the dance:
    in_json = add_url_and_capture_date(in_json, event)
    
    # Exclui entradas não-desejadas:
    in_json = [delete_keys_from_dict(dictionary, event) for dictionary in in_json]
    
    # Seleciona informações relevantes que estavam acima do nível selecionado
    # acima:
    in_json, save_to_s3 = preserve_root_data(in_json, event)
    
    return in_json, save_to_s3

        
def response_to_dict_list(event, response):
    """
    Translate the GET response to a list of dictionaries.
    """
    # Caso o dado seja de um tipo especial (e.g. html do DOU):
    if event['data_type'] == 'external_module':
        if debug:
            print ('data_type = external')
        em = importlib.import_module(event['name'].replace('-', '_'))
    
        in_json = em.entrypoint(response, event)
        if debug:
            print('len(in_json):', len(in_json))
        save_to_s3 = True
        
        # Add capture date and url:
        in_json = add_url_and_capture_date(in_json, event)
        
    # Caso o dado seja dos dados abertos do congresso e tal:
    else:   
        # Prepara o arquivo baixado (response) em json:
        data = load_as_json(event, response)
    
        # Seleciona os dados desejados e joga outros fora:
        # (in_json é uma lista de dicionários).
        in_json, save_to_s3 = filter_data(event, data)

    return in_json, save_to_s3


def write_to_s3(event, response):
    """
    Teoricamente deveriam ser duas funções:
    -- Select data;
    -- Prepare data;
    -- To json;
    -- write to s3 mesmo.
    """
    if debug:
        print(event['bucket'], event['key'])
    
    # Translate the response from GET to a list of dictionaries:
    in_json, save_to_s3 = response_to_dict_list(event, response)
    
    # Se não for pra salvar ou dados estiverem vazios, vai embora:
    if not save_to_s3:
        if debug:
            print('save_to_s3 = False: do not save to S3.')
        return None 
    if not len(in_json):
        if debug:
            print('len(in_json)=0: do not save to S3.')
        return None

    # record é dicionário, json abaixo é pacote do python.
    # Transforma a lista de dicionários em lista de jsons:
    # (json é uma string).
    if debug:
        print ('Creating json list...')
    result = [json.dumps(record, ensure_ascii=False) for record in in_json] 
    
    # Cria um arquivo texto com vários jsons:
    body = '\n'.join(result)
    
    # Salva no S3 os jsons:
    if debug:
        print('Putting object in S3 bucket...')
    client = boto3.client('s3')
    s3_log = client.put_object(
                  Body=body,
                  Bucket=event['bucket'], 
                  Key=event['key'])
    if debug:
        print('s3_log:', s3_log)
    
    return s3_log['ResponseMetadata']['HTTPStatusCode']


def load_params(event):
    """
    Pega no dynamo um dicionário especificado pelo order no 
    event.
    
    Sample input : dict
        {'dynamo_table_name': 'temp-capture-camara-tramitacoes-live-2020-06-23-16-30-26', 'order': 5}
        (the dict containing the DynamoDB temp table and the Item's key 'order')
    """
    # Similar a um Client de dynamo, para acessar as tabelas:
    dynamodb = boto3.resource('dynamodb')
    
    # Pega a tabela do dynamo especificada em event:
    table = dynamodb.Table(event['dynamo_table_name'])
    
    # Pega a "linha" da tabela do dynamo dada pela 'order' no `event`: 
    # (linha é um dicionário, na verdade):
    response = table.get_item(Key={'order': event['order']})

    # Carrega o dicionário para `response`:
    response = dyjson.loads(response)
    
    # Além do item, o .get_item também devolve metadados. Aqui 
    # pegamos apenas o item mesmo:
    return response['Item']
    # Essa função retorna um dicionário com estrutura similar a descrita 
    # no docstring da função `lambda_handler`, na parte "For testing purposes".

  
def copy_s3_to_storage_gcp(order, bucket, key):
    """
    Inputs:
    - 'order': an int that specifies the target url in a temp table;
    - 'bucket': the bucket in which the file is stored in AWS and GCP;
    - 'key': the "file path" of the file.
    
    This function calls the lambda function that copies the file from AWS
    to GCP storage.
    """
    params = {'order': order, 'bucket': bucket, 'key': key}
    
    lambd = boto3.client('lambda')
    
    if params['order'] >= 0:
        if debug:
            print('Invoking write-to-storage-gcp...')
        # Order lambda to save this result to storage (Google):
        try:
            lambd.invoke(
                FunctionName='arn:aws:lambda:us-east-1:085250262607:function:write-to-storage-gcp:JustLambda',
                InvocationType='Event',
                Payload=json.dumps(params))
        except(EndpointConnectionError):
            print('Failed to call write-to-storage-gcp')
            return 2
    
        return 200
    
    # order < 0:
    print('Should never get here')
    return 3
    

def register_captured_url_aws(table_name, url):
    """
    Put the `url` (str) as a new item in AWS DynamoDB table 
    `table_name` (str).
    
    The purpose of this function is to keep track of files (urls) already
    downloaded.
    """
    # Pega a referência (pointer) da tabela do dynamo:    
    dynamodb = boto3.resource('dynamodb')
    table    = dynamodb.Table(table_name)

    # Registra o `url` como um novo item na tabela do DynamoDB: 
    with table.batch_writer() as batch:
        batch.put_item(Item={'url': url})


def get_and_save(params, event):
    """
    Input:
    - 'params': a dict with a dynamoDB temp table name and an position ('order') of a 
      data in the table;
    - 'event': a dict with lots of info about the data location, type, parts to extract, 
      where to save it, etc. This was loaded from the dynamoDB temp table.
      
    Sample input
    ------------
    
    params : dict
        {'dynamo_table_name': 'temp-capture-camara-tramitacoes-live-2020-06-23-16-30-26', 'order': 5}
    
    event : dict
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

    PS: `event` might contain other keys not listed above. This depends on the 
    kind of data being downloaded.
      
    This function downloads the data and save it to AWS S3 and Google Storage.
    Basically, the data is captured from `event['url']` and saved to 
    `event['key']` (in bucket `event['bucket']`).
    """ 
    
    # Set max_retries for HTTP GET to 3 (instead of one):
    # This makes the download more robust.
    session = requests.Session()
    session.mount('http', requests.adapters.HTTPAdapter(max_retries=3))
    
    # Pega o arquivo especificado pelo url no event:
    if 'url' in event.keys():
        if debug:
            print('GET file...')
        response = session.get(event['url'], 
                               params=event['params'], 
                               headers=event['headers'], # configs para HTTP GET.
                               timeout=30)
    # Algumas capturas (e.g. tweets) não possuem url. Nesse caso, apenas 
    # continua abaixo:
    else:
        response = None
        
    # Se captura ocorreu bem ou se ainda vai capturar (no caso sem url),
    # salva na AWS S3 e Google Storage:
    if response == None or (response != None and response.status_code == 200):
        if debug:
            if response != None:
                print('GET successful. Writing to S3...')
            else:
                print('Will obtain non-http-get data...')
        # Salva arquivo baixado no S3 (Amazon), além de outras coisas:
        # (também registra o destino do arquivo)
        status_code_s3 = write_to_s3(event, response)
        if debug:
            print('write_to_s3 status code:', status_code_s3)

        # Copy the result to GCP storage:
        status_code_gcp = 10
        if status_code_s3 == 200:
            status_code_gcp = copy_s3_to_storage_gcp(params['order'], event['bucket'], event['key'])

        # Registra url capturado em tabela do dynamo, se tal ação for requisitada.
        # Isso acontece no caso da captura de matérias do DOU. O motivo para 
        # guardarmos quais matérias foram baixadas é que as matérias podem ser 
        # publicadas em horários diferentes e o site do DOU pode sair do ar.
        # Para não perder nenhuma matéria, vamos registrando quais do dia de hoje 
        # já baixamos:
        if 'url_list' in event['aux_data'].keys():
            if status_code_gcp == 200:
                if debug:
                    print('Register sucessful capture on table' + event['aux_data']['url_list'])
                register_captured_url_aws(event['aux_data']['url_list'], event['url'])
            elif debug:
                print('Capture failed for ' + event['url'])

        # TODO: colocar como lidar com erros no GET.
    
    # Retorna a resposta do http GET para poder pegar as próximas levas (páginas)
    # dos dados, caso eles estejam paginados (como é o caso da API da câmara):
    return response
    

def get_next_page(event, response):
    """
    Input:
    - 'event':    a dict with all info about the data to be captured and saved;
    - 'response': a response from a HTTP GET request.
    
    For APIs that return paginated data, get from the last request response the 
    next page to be downloaded.
    
    Returns: a dict with:
    - 'key' (the file path where to save the data's next page);
    - 'url' (the address of the data's next page).
    """
    
    if debug:
        print('Checking for pagination in data.')
    
    # Parse JSON:
    raw_data = load_as_json(event, response)
    
    # Case Dados Abertos da câmara dos deputados:
    if 'links' in raw_data.keys():
        
        # Look for next page link:
        next_url_set = {d['href'] for d in filter(lambda d: d['rel'] == 'next', raw_data['links'])}
        if len(next_url_set) > 1:
            raise Exception('Unexpected case: more than one "next" link.')
        # If found, get link and set its key:
        elif len(next_url_set) == 1:
            next_url = list(next_url_set)[0]
            page_num = re.search('pagina=(\d+)', next_url).group(1)
            next_key = re.sub('(_p\d+)?\.json', '_p' + page_num + '.json', event['key'])
            
            return {'key': next_key, 'url': next_url}
    
    # Default case:
    return None
    

def call_next_step(params):
    """
    According to the configuration in params:
    -- If this is the last entry in dynamo temp table, finish and delete temp table;
    -- Else, restart the process (call Lambda 'http-request') with lower order
       (order = order - 1)
    (next GET target).
    
    Sample input
    ------------
    
    params : dict
        {'dynamo_table_name': 'temp-capture-camara-tramitacoes-live-2020-06-23-16-30-26', 'order': 5}
        (DynamoDB temp table and Item's key 'order').
    """
    
    # Instantiate a Lambda client (to call a Lambda function):
    lambd = boto3.client('lambda')
    
    # If this is the last Item, delete temp table:
    if params['order'] <= 0:
        
        # debug:
        #return 0
        
        if debug:
            print('Deleting DynamoDB')
        
        # Call delete dynamodb table (temp):
        lambd.invoke(
            FunctionName='arn:aws:lambda:us-east-1:085250262607:function:dynamodb-delete-table:JustLambda',
            InvocationType='Event',
            Payload=json.dumps(params))    
    
    # If this is not the last Item in the dynamoDB table, get the next one.
    else:
         
        # Get next item key:
        params['order'] = params['order'] - 1
        
        if debug:
            print('Calling order: ', params['order'])   
        
        if local:
            lambda_handler(params, {})
        else:
            # Call this same function again, but with lower order: 
            lambd.invoke(
             FunctionName='arn:aws:lambda:us-east-1:085250262607:function:http-request:JustLambda',
             #FunctionName='arn:aws:lambda:us-east-1:085250262607:function:http-request:DEV',
             InvocationType='Event',
             Payload=json.dumps(params))    

            
def lambda_handler(params, context):
    """
    Downloads the data mentioned in an Item identified by the key 'order' of a 
    DynamoDB's temp table (created by Lambda function 'parametrize-API-requests').
    The temp table and 'order' are described in `params`. The data is saved 
    to AWS S3 and Google Storage. In some rarer cases, the data is processed by 
    python scripts in folder `external_modules` before saving it. 
    
    Sample Input
    ------------
    
    params : dict
        {'dynamo_table_name': 'temp-capture-camara-tramitacoes-live-2020-06-23-16-30-26', 'order': 5}
        (the dict containing the DynamoDB temp table and the Item's key 'order')
    
        For testing purposes, the `params` input can be the dict that would be stored 
        in a DynamoDB table item, directly, e.g.:
        
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
    
    context : empty dict 
        {} (not used)
    """
    
    print(params)
    
    # Para poder identificar os erros que acontecerão no dynamo:
    dynamo_exceptions = boto3.client('dynamodb').exceptions
        
    try:
        # Input `params` default (temp table):
        if 'dynamo_table_name' in params.keys():           
            if debug:
                print('Loading params from dynamo temp table...')
            # Carrega dicionário do dynamo:
            event = load_params(params)
        # For debugging:
        else:
            if debug:
                print('Assuming `params` is a typical data in dynamo temp table item.')
            # Se não existe referência à tabela no dynamo, assume que esse é o próprio dicionário
            # (opção para debugging):
            event = params
            params = {'order': 0}

        # Download data and save it to AWS and GCP:
        response  = get_and_save(params, event)
        
        # A API dos dados abertos da Câmara retorna os dados paginados (máximo de 
        # 100 dados por vez, se não me engano. Se for esse caso, pega próximas
        # páginas até esgotar os dados solicitados:
        next_page = get_next_page(event, response)
        # While there are more pages, get it and repeat the capture process:
        while next_page != None:
            # Update event for next page:
            event['key'] = next_page['key']
            event['url'] = next_page['url']
    
            # Get next page of data:
            response  = get_and_save(params, event)
            next_page = get_next_page(event, response) 
    
    # Possível erro: não encontrou a tabela temp no DynamoDB:
    except dynamo_exceptions.ResourceNotFoundException:
        
        print('DynamoDB Table does not exist')    
        return # force exit 
    
    # Algum outro possível erro:
    except Exception as e:
        
        # Raise error somewhere, maybe slack
        print(e)

    # A função abaixo chama este Lambda recursivamente, reduzindo o key 'order',
    # até esgotar todos os arquivos listados na tabela temp do DynamoDB:
    call_next_step(params)
