# event – AWS Lambda uses this parameter to pass in event data to the handler. 
# This parameter is usually of the Python dict type. It can also be list, str, 
# int, float, or NoneType type.

# EXAMPLE of the expected input for this function:
{'table_name': 'capture_urls', 'key': {'name': {'S': 'camara-tramitacoes'}, 'capture_type': {'S': 'live'}}}