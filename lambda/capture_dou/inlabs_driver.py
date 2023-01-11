import requests

class InLabsDriver:

    def __init__(self):
        self._LOGIN = "adm.gabinetecompartilhado@gmail.com"
        self._PASS = "@c3ssogabinete"
        self.url_login = "https://inlabs.in.gov.br/logar.php"
        self.url_download = "https://inlabs.in.gov.br/index.php?p="
        self.payload = {"email" : self._LOGIN, "password" : self._PASS}
        self.headers = {
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                        }
        self.session = requests.Session()

    def login(self):
        try:
            response = self.session.request("POST", self.url_login, data=self.payload, headers=self.headers, timeout=10)
        except requests.exceptions.SSLError:
            response = self.session.request("POST", self.url_login, data=self.payload, headers=self.headers, timeout=10, verify=False)

