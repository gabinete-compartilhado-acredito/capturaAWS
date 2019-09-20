import boto3
from dynamodb_json import json_util as dyjson 
from pyathena import connect
from datetime import timedelta, date, datetime
from collections import defaultdict
import time
import json
import random
import sys
sys.path.insert(0, "external_modules")
import importlib

# Switch for printing messages to log:
debug = False

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

def forms_date_start_end(par, item, forms):
    """
    A partir de um modelo de URL e de filename, cria realizações concretas 
    substituindo cada um das datas listadas como input nos URLs e filenames.
    As datas tem formato definido por date_format que vem no input.
    """
    
    def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
            yield start_date + timedelta(n)
    
    end_date = date.today() if par['end_date'] == 'now' else datetime.strptime(par['end_date'], par['date_format'])

    start_date = end_date + timedelta(par['timedelta'])
    
    for single_date in daterange(start_date, end_date):
        dates = {'start_date': single_date, 'end_date': single_date + timedelta(1)}
        forms.append({'url': item['url'] % {key: datetime.strftime(value, par['date_format']) for key, value in dates.items()},
            'filename': '_'.join([item['name'],
                                  datetime.strftime(dates['start_date'], '%Y-%m-%d'),
                                  datetime.strftime(dates['end_date'], '%Y-%m-%d'),]) + '.json'})
    
    return forms

def forms_external_module(par, item, forms):
    
    em = importlib.import_module(item['name'].replace('-', '_'))
    
    return em.entrypoint(par)


def generate_forms(item, event):
    """
    Cria URLs a partir das informações no dynamo.
    
    Retorno: forms, que é basicamente uma lista de dicionários que 
    cada dicionário contém um URL e uma filename (destino).
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
    
        body.append(
            dict(url=item['url'],
                 params={},
                 headers=response['Item']['headers'],
                 bucket=response['Item']['bucket'],
                 key=response['Item']['key'] + item['filename'],
                 data_type=response['Item']['data_type'],
                 data_path=response['Item']['data_path'],
                 exclude_keys=response['Item']['exclude_keys'],
                 records_keys=response['Item']['records_keys'],
                 name=response['Item']['name']
                )
        )
        
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


def lambda_handler(event, context):
    """
    Cria lista de de URLs para baixar, e depois chama o lambd.invoke que 
    efetivamente baixa o conteúdo dos URLs.
    
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
    
    # Cria cliente do dynamo
    client = boto3.client('dynamodb')
    
    # Seleciona um arquivo do dynamo:
    response = client.get_item(TableName=event['table_name'], 
                                Key=event['key'])
    
    # Lê o arquivo do dynamo:
    response = dyjson.loads(response)
    if debug == True:
        print("dict of dynamo Table:") 
        print(response)
    
    # Gera as URLs e os filenames (destino):
    body = generate_body(response, event)
    
    # Salva os as informações geradas acima no dynamo como uma tabela temp:
    params = create_and_populate_dynamodb_table(body, event)
    if debug == True:
        print('URLs to capture listed in:')
        print(params)
    
    # Chama cliente do lambda:
    lambd = boto3.client('lambda')
    
    # Faz a caputa efetivamente, com os parâmetros criados por generate_body e 
    # salvos por create_and_populate_dynamodb_table:
    
    #return 0
    
    lambd.invoke(
        FunctionName='arn:aws:lambda:us-east-1:085250262607:function:http-request:JustLambda',
        #FunctionName='arn:aws:lambda:us-east-1:085250262607:function:http-request:DEV',
        InvocationType='Event',
        Payload=json.dumps(params))