import requests
import boto3
import json
class InLabsDriver:

    def __init__(self):
        self.url_login = "https://inlabs.in.gov.br/logar.php"
        self.url_download = "https://inlabs.in.gov.br/index.php?p="
        self.headers = {
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                        }
        self.session = requests.Session()

    def get_cookies(self):
        try:
            cookie = self.session.cookies.get('inlabs_session_cookie')
        except:
            raise Exception("Failed to obtain cookies. Verify credentials")
        
        self.cookie = cookie

    def login(self):
        # Retrieve login and password from AWS
        s3 = boto3.client('s3')
        obj = s3.get_object(
                      Bucket='config-lambda', 
                      Key='inlabs_credentials.json')
        
        secret = json.loads(obj['Body'].read().decode('utf-8'))
        email = secret['email']
        password = secret['pass']

        # Use login and password in the payload
        self.payload = {"email" : email, "password" : password}

        try:
            response = self.session.request("POST", self.url_login, data=self.payload, headers=self.headers, timeout=10)
        except requests.exceptions.SSLError:
            response = self.session.request("POST", self.url_login, data=self.payload, headers=self.headers, timeout=10, verify=False)
        
        # Obtaining cookies
        self.get_cookies()



