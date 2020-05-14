import joblib
import pandas as pd
import boto3
import req_classifier

from sklearn.metrics import accuracy_score

def save_to_tmp(bucket, key, string=True):
    """
    Get a file from AWS S3 and save it to /tmp/ folder.
    
    Input
    -----
    
    bucket : str
        The AWS bucket where the file is stored.
        
    key : str
        The path to the file in the AWS bucket.
    
    string : bool (default True)
        Wheter or not the data in file is text.
    """
    mode = 'w' if string else 'wb'
    
    # Get data:
    s3 = boto3.client('s3')
    a = s3.get_object(Bucket=bucket, Key=key)
    data = a['Body'].read()
    if string:
        data = data.decode('utf-8')
        
    # Save to temp folder:
    filename = '/tmp/' + key.split('/')[-1]
    with open(filename, mode) as f:
        f.write(data)
    
    return filename
    

def lambda_handler(event, context):
    
    # Load model:
    model_file = save_to_tmp('config-lambda', 'models/req_classifier_svm_2020-03-17/tested_model.joblib', string=False)
    best_pipeline = joblib.load(model_file)
    
    return 
    
    # Load data:
    train_file = save_to_tmp('config-lambda', 'models/req_classifier_svm_2020-03-17/train_data.csv')
    train_df = pd.read_csv(train_file)
    
    test_file = save_to_tmp('config-lambda', 'models/req_classifier_svm_2020-03-17/test_data.csv')
    test_df  = pd.read_csv(test_file)
    
    # Predict:
    pred_train = best_pipeline.predict(train_df.ementa)
    print('Accuracy on training data:', accuracy_score(train_df['class'], pred_train))
    
    pred_test = best_pipeline.predict(test_df.ementa)
    print('Accuracy on test data:', accuracy_score(test_df['class'], pred_test))
