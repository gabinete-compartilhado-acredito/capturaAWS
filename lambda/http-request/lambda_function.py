import json
#from botocore.vendored import requests
import requests
import boto3
import xmltodict
from collections import MutableMapping
from datetime import datetime
from dynamodb_json import json_util as dyjson 
import re
import sys
sys.path.insert(0, "external_modules")
import importlib

# For debugging:
debug = False

def get_nested_dict(data, keys):

    for key in keys:

        data = data[key]
    
    return data
    
    
def add_url_and_capture_date(in_json, event):
    
    # add url to all records
    if not isinstance(in_json, list):
        in_json = [in_json]
        
    for record in in_json:
        record.update({
            'api_url': event['url'],
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
    
        in_json = em.entrypoint(response, event['url'])
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
    """
    # Similar ao client de dynamo:
    dynamodb = boto3.resource('dynamodb')
    
    # Pega a tabela do dynamo especificada em event:
    table = dynamodb.Table(event['dynamo_table_name'])
    
    # Lega "linha" da tabela do dynamo com order dado pelo event 
    # (linha é um dicionário, na verdade):
    response = table.get_item(Key={'order': event['order']})

    # Carrega o dicionário:
    response = dyjson.loads(response)
    
    # Além do item, o .get_item também devolve metadados. Aqui 
    # pegamos apenas o item mesmo:
    return response['Item']

  
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
        # Order lambda to save this result to storage (Google):
        lambd.invoke(
         FunctionName='arn:aws:lambda:us-east-1:085250262607:function:write-to-storage-gcp:JustLambda',
         InvocationType='Event',
         Payload=json.dumps(params))

 
def get_and_save(params, event):
    """
    Input:
    - 'params': a dict with a dynamoDB temp table name and an position ('order') of a 
      data in the table;
    - 'event': a dict with lots of info about the data location, type, parts to extract, 
      where to save it, etc.
      
    This function downloads the data and save it to AWS S3 and Google Storage.
    """ 
    # Set max_retries for HTTP GET to 3:
    session = requests.Session()
    session.mount('http', requests.adapters.HTTPAdapter(max_retries=3))
    
    # Pega o arquivo especificado pelo url no event:
    if debug:
        print('GET file...')
    response = session.get(event['url'], 
                           params=event['params'], 
                           headers=event['headers'], # configs para HTTP GET.
                           timeout=30)

    # Rodou bem:                       
    if response.status_code == 200:
        if debug:
            print('GET successful. Writing to S3...')
        # Salva arquivo baixado no S3 (Amazon), além de outras coisas:
        # (também registra o destino do arquivo)
        status_code_s3 = write_to_s3(event, response)
        if debug:
            print('write_to_s3 status code:', status_code_s3)

        # Copy the result to GCP storage:
        copy_s3_to_storage_gcp(params['order'], event['bucket'], event['key'])

        # TODO: colocar como lidar com erros no GET.
    
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
    -- Copy downloaded file from S3 to Google storage (deactivated, passed to outside);
    -- If this is the last entry in dynamo temp table, finish and delete temp table;
    -- Else, restart the process with lower order (next GET target).
    """
    
    lambd = boto3.client('lambda')
    
    # check if loop is done
    if params['order'] <= 0:
        
        if debug:
            print('Deleting DynamoDB')
        
        # call delete dynamodb table (temp)
        lambd.invoke(
            FunctionName='arn:aws:lambda:us-east-1:085250262607:function:dynamodb-delete-table:JustLambda',
            InvocationType='Event',
            Payload=json.dumps(params))    
    
    else:
         
        # call next lambda
        params['order'] = params['order'] - 1
        
        if debug:
            print('Calling order: ', params['order'])   
        
        # Call this same function again, but with lower order: 
        lambd.invoke(
         FunctionName='arn:aws:lambda:us-east-1:085250262607:function:http-request:JustLambda',
         #FunctionName='arn:aws:lambda:us-east-1:085250262607:function:http-request:DEV',
         InvocationType='Event',
         Payload=json.dumps(params))
    

def lambda_handler(params, context):
    """
    Downloads the files in dynamo's temp table described in config file 
    params.
    """
    
    print(params)
    
    # Para poder pegar os erros que acontecerão no dynamo:
    dynamo_exceptions = boto3.client('dynamodb').exceptions
        
    try:
        if debug:
            print('Loading params...')
        # Carrega dicionário do dynamo:
        event = load_params(params)

        # Download data and save it to AWS and GCP:
        response  = get_and_save(params, event)
        next_page = get_next_page(event, response)

        # While there are more pages, get it and repeat the capture process:
        while next_page != None:
            # Update event for next page:
            event['key'] = next_page['key']
            event['url'] = next_page['url']
    
            # Get next page of data:
            response  = get_and_save(params, event)
            next_page = get_next_page(event, response) 
    
    except dynamo_exceptions.ResourceNotFoundException:
        
        print('DynamoDB Table does not exist')    
        return # force exit 
    
    except Exception as e:
        
        # Raise error somewhere, maybe slack
        print(e)
        
    call_next_step(params)
