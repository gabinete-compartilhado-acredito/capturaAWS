from slackclient import SlackClient
import json


blocks = """[
    {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "*%(descricao_post)s*"
        }
    },
    {
        "type": "divider"
    },
        %(fields)s
]"""

# Below is a model for a post in slack, properly formatted.
# It is a string where python will replace place-holders 
# for variables (that start with %) with actual values:
field = """
    {
        "type": "section",
        "fields": [
{
                "type": "mrkdwn",
                "text": "\
<%(url_proposicao)s|*%(tipo)s %(numero)s / %(ano)s*>\
\n*Autor:*\n%(autor)s - %(partido_autor)s\n
*Ementa:*\n%(ementa)s\n\
<%(url_inteiro_teor)s|*Inteiro Teor*>"
            },
            {
                "type": "mrkdwn",
                "text": ".\n\
*Status:*\n%(status)s\n
*Órgão:*\n%(orgao)s\n\
*Despacho:*\n%(despacho)s\n"
            }
        ]
    }"""

# For posts of DOU:
fieldDOU = """
    {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "*%(identifica)s*\\n *Órgão*: %(orgao)s\\n\\n *%(resumo_tipo)s:* %(resumo)s\\n\\n"
        }
    },
    {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "*Assina:* %(assina)s (%(cargo)s)\\n*Publicado em:* %(pub_date)s | Edição: %(edicao)s | Seção: %(secao)s | Página: %(pagina)s\\n<%(url)s|Artigo completo>  |  <%(url_certificado)s|Versão certificada>\\n\\n\\n"
            }
        ]
    }
"""

# For system messages:
fieldSys = """	
    {
		"type": "section",
		"text": {
			"type": "mrkdwn",
			"text": "*%(function)s:* %(message)s"
		}
	}
"""

def lambda_handler(event, context):
    """
    Get the payload posted by Amazon's SNS as 'event'
    """

    # Transform the SNS message (a JSON) into a dict:
    event = json.loads(event['Records'][0]['Sns']['Message'])
    
    # Create a series of slack posts (one for each new tramitação, separated 
    # by commas) using the info in event data:
    if event['casa'] == 'dou':
        fields = ','.join(map(lambda x: fieldDOU % x, event['data']))
    elif event['casa'] == 'sys':
        fields = ','.join(map(lambda x: fieldSys % x, event['data']))
    else:
        fields = ','.join(map(lambda x: field % x, event['data']))
    
    if len(fields) == 0:
        print('No updates')
        return 0
        
    event.update({'fields': fields})
    
    # Username and password for Slack:
    with open('slack_token.pass', 'r') as token_file:
        slack_token = token_file.read()
    sc = SlackClient(slack_token)
    
    res = sc.api_call(
      "chat.postMessage",
      channel=event['media']['channel'],
      blocks=blocks % event
    )
    
    if not res['ok']:
        print('Call to Slack post message failed!')
        print(res)
        print(event)
    