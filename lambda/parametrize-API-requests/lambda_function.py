import boto3
from dynamodb_json import json_util as dyjson 
from pyathena import connect
from datetime import timedelta, date, datetime
from collections import defaultdict
import time
import json
import random
import sys
import google.auth
from google.cloud import bigquery
import os
sys.path.insert(0, "external_modules")
import importlib

# Switch for printing messages to log:
debug = False
# Wheter this code is ran locally or on AWS:
local = False


def query_bigquery(query):
    """
    Runs a `query` (str) on Google BigQuery and returns the results as
    a list of dictionaries.
    """
    
    if local:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/key.json'
    
    # Get key for accessing BigQuery:
    s3 = boto3.client('s3')
    a  = s3.get_object(
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
        
    result = bq.query(
        query,
        # Location must match that of the dataset(s) referenced in the query.
        location="US",
    )  # API request - starts the query
    
    result = [dict(r.items()) for r in result] 
    
    return result


def forms_bigquery(par, item, forms):
    """
    Faz um query no Google BigQuery e usa os resultados para 
    construir uma lista de URLs e filenames (destino).
    """
       
    # Substitui parâmetros de input na query:
    query = par['query'] % par['query_config']
    
    # Executa a query:
    data = query_bigquery(query)
        
    # LOOP sobre as linhas do retorno do SQL:
    for d in data:
        
        # Create data destination filename:
        if len(d) > 1:
            end_filename = '&'.join(map(lambda x: '='.join(map(str, x)), zip(par['url_params'], list(d.values()))))
        else:
            end_filename = d.values()[0]
        filename = '_'.join(map(str, [item['name'], end_filename])) + '.json'
        
        # Create source url:    
        url = item['url'] % dict(zip(par['url_params'], list(d.values())))
        
        if 'url' in d:
            raise Exception("'url' key already exists in data; avoiding its redefinition.")
        if 'filename' in d:
            raise Exception("'filename' key already exists in data; avoiding its redefinition.")
        d['url']      = url
        d['filename'] = filename
        
        forms.append(d)
    
    return forms


def forms_athena_query(par, item, forms):
    """
    Faz um query no Athena (SQL da Amazon) e usa os resultados para 
    construir uma lista de URLs e filenames (destino).
    """
    
    # Get AWS security credentials:
    client = boto3.client('s3')
    a = client.get_object(Bucket='config-lambda', Key='aws_accessKeys.json')
    aws_key = json.loads(a['Body'].read().decode('utf-8'))

    # Conecta à Athena com pacote do Joe.
    cursor = connect(aws_access_key_id=aws_key['aws_access_key_id'],
                         aws_secret_access_key=aws_key['aws_secret_access_key'],
                         s3_staging_dir='s3://stagging-random/',
                         region_name='us-east-1').cursor()
    
    # Substitui parâmetros de input na query:
    query = par['query'] % par['query_config']
    
    # Executa a query:
    data = cursor.execute(query).fetchall() 
    
    # LOOP sobre as linhas do retorno do SQL:
    for d in data:
        
        if len(d) > 1:
            end_filename = '&'.join(map(lambda x: '='.join(map(str, x)),
                                                  zip(par['url_params'], 
                                                      list(d))))
        else:
            end_filename = d[0]

        forms.append({'url': item['url'] % dict(zip(par['url_params'], list(d))),
                      'filename': '_'.join(map(str, [item['name'], end_filename])) + '.json'
                      })
    
    return forms


def forms_from_to(par, item, forms):
    """
    A partir de um modelo de URL e de filename, cria realizações concretas 
    substituindo cada um dos anos listados como input nos URLs e filenames.
    
    Dynamodb data structure:
    {
      "body": {
        "from": 1993,
        "to": 2019
      },
      "name": "id",
      "type": "from_to"
    }
    """
    
    # LOOP sobre os anos:
    for year in range(par['body']['from'], par['body']['to'] + 1):
        
        forms.append({'url': item['url'] % {par['name']: year},
                      'filename': '_'.join(map(str, [item['name'], year])) + '.json'
                      })
    
    return forms
   
    
def forms_from_external_list(par, item, forms, event):
    
    for item_from_list in event['external_params']['list']:
        
        forms.append({'url': item['url'] % {par['url_param']: item_from_list},
                      'filename': '_'.join(map(str, [item['name'], item_from_list])) + '.json'
                      })
    
    return forms


def daterange(start_date, end_date):
    """
    Given a 'start_date' and an 'end_date' (datetimes), returns a generator
    for dates in that range, with the same behaviour as 'range' (i.e. excludes 
    the 'end_date' from the returned values).
    
    NOTE: if 'start_date' > 'end_date', it returns the dates from 'end_date' 
    to 'start_date', excluding 'start_date' instead of 'end_date'. In other
    words, it always excludes the farthest future date.
    """
    if end_date - start_date < timedelta(0):
        temp_date  = end_date
        end_date   = start_date
        start_date = temp_date
    
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)


def forms_date_start_end(par, item, forms):
    """
    A partir de um modelo de URL e de filename, cria realizações concretas 
    substituindo cada um das datas listadas como input nos URLs e filenames.
    As datas tem formato definido por date_format que vem no input.
    """
    
    # Parse relative or specified capture's end date: 
    if par['end_date'] == 'yesterday':
        end_date = date.today() - timedelta(1)
    elif par['end_date'] == 'now':
        end_date = date.today()
    else:
        end_date = datetime.strptime(par['end_date'], par['date_format'])

    start_date = end_date + timedelta(par['timedelta'])
    
    for single_date in daterange(start_date, end_date):
        dates = {'start_date': single_date, 'end_date': single_date + timedelta(1)}
        
        # Create filename for data:
        # In case both dates are required in the url:
        if (item['url'].find('start_date') != -1) and (item['url'].find('end_date') != -1):
            filename = '_'.join([item['name'],
                                          datetime.strftime(dates['start_date'], '%Y-%m-%d'),
                                          datetime.strftime(dates['end_date'], '%Y-%m-%d'),]) + '.json'
        # In case only the start date is required in the url:
        elif item['url'].find('start_date') != -1:
            filename = '_'.join([item['name'], datetime.strftime(dates['start_date'], '%Y-%m-%d')]) + '.json'
        # In case only the end date is required in the url:
        elif item['url'].find('end_date') != -1:
            filename = '_'.join([item['name'], datetime.strftime(dates['end_date'], '%Y-%m-%d')]) + '.json'
        # In case no dates are required in the URL:
        else:
            filename = item['name'] + '.json'

        forms.append({'url': item['url'] % {key: datetime.strftime(value, par['date_format']) for key, value in dates.items()},
            'filename': filename})
    
    return forms


def forms_external_module(par, item, forms):
    
    em = importlib.import_module(item['name'].replace('-', '_'))
    
    return em.entrypoint(par)


def generate_forms(item, event):
    """
    Cria URLs a partir das informações no dynamo.
    
    Retorno: forms, que é basicamente uma lista de dicionários que 
    cada dicionário contém um URL e uma filename (destino).
    
    Input
    -----
    
    item : dict
        Este é o conteúdo de um item da tabela DynamoDB capture_urls, isto é,
        um JSON de configuração de captura.
        
    event : dict
        Este é o input da função principal lambda_handler.
    """
    
    # Pega entrada 'parameters' no arquivo do dynamo:
    parameters = item['parameters']
    
    forms = []
    for par in parameters:
        print(par)
        
        # Verifica o tipo de tarefa e executa o código apropriado:
        if par['type'] == 'from_to':
            
            forms = forms_from_to(par, item, forms)

        elif par['type'] == 'date_start_end':

            forms = forms_date_start_end(par, item, forms)
        
        elif par['type'] == 'athena_query':

            forms = forms_athena_query(par, item, forms)
        
        elif par['type'] == 'bigquery':
            forms = forms_bigquery(par, item, forms)
            
        elif par['type'] == 'external_list':
            
            form = forms_from_external_list(par, item, forms, event)
        
        elif par['type'] == 'empty':
            
            forms = [{'url': item['url'],
                      'filename': item['name'] + '.json'
                     }]
        
        elif par['type'] == 'external_module':
            forms = forms_external_module(par['params'], item, forms)
        
        else:

            raise 'Parameter type not identified'
            
    return forms 
    
    
def generate_body(response, event):
    """
    Gera as URLs a partir de informações em arquivo 'response' do dynamo,
    e outras coisas (metadados necessários).
    
    Input
    -----
    
    response : dynamoDB.get_item response
        The content of the item in dynamoDB capture_urls table 
        is stored in response['Item'].
        
    event : dict
        The input of the main function lambda_handler.
    """
    
    # Gera as URLs:
    forms = generate_forms(response['Item'], event)
    
    # O response item é um dicionário. Aqui incluímos o default para 
    # não dar pau se faltar alguma key do dicionário (e.g. records_keys)
    response['Item'] = defaultdict(lambda: None, response['Item'])

    # Vamos popular uma lista de dicionários 'body' com URLs e metadados:    
    body = []
    for item in forms:
    # Do item vem filename e url, o resto vem do dynamo, basicamente infos 
    # sobre localização dos dados.
    
        # Este vai ser o dicionário em cada linha da tabela temp do dynamoDB
        # que a Lambda http-request vai carregar e utilizar como informação 
        # para realizar o download.
        request_pars = dict(url=item.pop('url'), # O url definido por generate_forms acima.
                            params={}, # Parâmetros do HTTP GET.
                            headers=response['Item']['headers'], # Headers do HTTP GET.
                            bucket=response['Item']['bucket'], # bucket onde salvar os dados baixados.
                            key=response['Item']['key'] + item.pop('filename'), # Path onde salvar os dados baixados.
                            data_type=response['Item']['data_type'], 
                            data_path=response['Item']['data_path'], # Caminho em uma árvore de dados (e.g. XML) até os dados desejados.
                            exclude_keys=response['Item']['exclude_keys'],
                            records_keys=response['Item']['records_keys'],
                            name=response['Item']['name'],
                            requests_pars=response['Item']['requests_pars'] # Parâmetros do item do capture_urls a serem passados à Lambda http-request.
                           )
        request_pars['aux_data'] = item # Parâmetros gerados por generate_forms a serem passados à Lambda http-request.
    
        body.append(request_pars)
        
    return body
    

def create_dynamo_temp_table(table_name, dynamodb):
    
    try:
        table = dynamodb.Table(table_name)
        table.table_status

    except:
        create_table_response = dynamodb.create_table(
            TableName= table_name,
            AttributeDefinitions=[{
            'AttributeName': 'order',
            'AttributeType': 'N'
            }],
            KeySchema=[{
                'AttributeName': 'order',
                'KeyType': 'HASH'
            }],
            BillingMode='PAY_PER_REQUEST'
        )
    
    
def create_and_populate_dynamodb_table(urls, event):
    """
    urls é uma lista de dicionários. Cada dicionário tem 
    entradas descritas em 'body' na função generate forms acima.
    """
    
    dynamodb = boto3.resource('dynamodb')
   
    # Determina o nome da tabela de output no dynamo a partir das informações de captura: 
    table_name = '-'.join(['temp-capture',
                            event['key']['name']['S'],
                            event['key']['capture_type']['S'],
                            datetime.strftime(datetime.now(), '%Y-%m-%d-%H-%M-%S')])
    
    # Cria uma tabela vazia no dynamo:
    create_dynamo_temp_table(table_name, dynamodb)

    time.sleep(60)
    
    # Pega a referência (pointer) da tabela do dynamo:    
    table = dynamodb.Table(table_name)

    # Escreve os dicionários criados pela função generate_body na tabela do dynamo: 
    # REAAALY FAST!
    with table.batch_writer() as batch:
        for order, url in enumerate(urls): 
            url.update({'order': order})   # Cria um novo key com a ordem dos dicionários 'url' na lista 'urls'.
            batch.put_item(Item=url)

    # Retorna o nome da tabela e o número de linhas - 1:    
    return {'dynamo_table_name': table_name, 'order': len(urls) - 1}


def adapt_url_key(body_entry):
    """
    Rename the `body_entry` dict key 'url' to 'identifier' 
    if its value does not start with 'http' or 'ftp'.
    
    PS: It changes the content of the input dict `body_entry`.
    """
    
    adapted = body_entry
    if body_entry['url'][:4] != 'http' and body_entry['url'][:3] != 'ftp':
        body_entry['identifier'] = body_entry.pop('url')
    
    return body_entry
    

def read_parallel_batches(response):
    """
    Given a `response` from dynamoDB's get_item (after translating from dyJSON, 
    that is, a dict where the important information, the content of a table's item, 
    is inside the key 'Item'), return the number of parallel batches in which to 
    split the requests. This is 1 by default, or something else if specified in the 
    dynamoDB item.
    """
    
    parallel_key = 'parallel_batches'
    config = response['Item']
    
    if parallel_key not in config.keys() or config[parallel_key] == None or config[parallel_key] <= 1:
        return 1
    
    else:
        return config[parallel_key]


def split_parallel_batches(body, n_batches):
    """
    Given a list `body` and an integer `n_batches`, tries to split `body` 
    into `n_batches` sub-lists. For certain combinations of parameters, 
    the number of sub-lists is different than the requested number `n_batches`.
    It is recommended to measure the length of the returned list of batches.
    """
    n_requests = len(body)
    
    # Set batch sizes:
    batch_sizes = [round(n_requests / n_batches) for i in range(n_batches)]
    batch_sizes[0] = max(n_requests - sum(batch_sizes[1:]), 0)

    # Set positions that mark the start and end of batches:
    batch_pos = [sum(batch_sizes[:i]) for i in range(n_batches + 1)]
    
    # Split into batches
    batches = [body[batch_pos[i]:batch_pos[i+1]] for i in range(n_batches) \
               if len(body[batch_pos[i]:batch_pos[i+1]]) > 0]
    
    return batches    
    

def lambda_handler(event, context):
    """
    Cria lista de de URLs para baixar, e depois chama o lambd.invoke que 
    efetivamente baixa o conteúdo dos URLs.
    
    # Exemplo de input em `event`:
    {
      "table_name": "capture_urls",
      "key": {
        "name": {
          "S": "camara-deputados-detalhes"
        },
        "capture_type": {
          "S": "historical"
        }
      }
    }"""
    
    
    print("Starting parametrize-API-requests with event:")
    print(event)

    # Cria cliente do dynamo:
    client = boto3.client('dynamodb')

    # Seleciona um arquivo do dynamo:
    response = client.get_item(TableName=event['table_name'], 
                                Key=event['key'])

    # Lê o arquivo do dynamo (retorna uma lista de dicionários ou um dicionário):
    response = dyjson.loads(response)
    if debug == True:
        print("dict of dynamo Table:") 
        print(response)

    # Gera as URLs e os filenames (destino):
    body = generate_body(response, event)
    if debug:
        print('# items to capture (body length):', len(body))
    # Rename 'url' key if it is not an url:
    body = [adapt_url_key(b) for b in body]

    # Split requests in parallel batches according to config:
    n_batches = read_parallel_batches(response)
    body_batches = split_parallel_batches(body, n_batches)

    # Chama cliente do lambda:
    lambd = boto3.client('lambda')

    
    for body in body_batches:  # If body_batches == [], it skips everything in the loop.
        print('Create dynamo temp table with', len(body), 'entries')

        # Salva os as informações geradas acima no dynamo como uma tabela temp:
        params = create_and_populate_dynamodb_table(body, event)
        if debug == True:
            print('URLs to capture listed in:')
            print(params)

        # Faz a captura efetivamente, com os parâmetros criados por generate_body e 
        # salvos por create_and_populate_dynamodb_table:    
        if True:
            if debug:
                print('Invoking http-request...')
            lambd.invoke(
                FunctionName='arn:aws:lambda:us-east-1:085250262607:function:http-request:JustLambda',
                #FunctionName='arn:aws:lambda:us-east-1:085250262607:function:http-request:DEV',
                InvocationType='Event',
                Payload=json.dumps(params))