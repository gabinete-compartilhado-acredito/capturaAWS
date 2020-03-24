# Entrypoint for processing data according to dynamodb config "classify_reqs_camara".

import pandas as pd

def process(code, input_data):
    """
    Process `input_data` (Pandas DataFrame) using hard-coded instructions below
    and the python object `code`.
    """
    predicted_class = code.predict(input_data.ementa)
    input_data['predicted_class'] = pd.Series(predicted_class, index=input_data.index)
    
    return input_data
