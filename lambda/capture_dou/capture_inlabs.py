import datetime as dt
from inlabs_driver import InLabsDriver

#tipo_dou="DO1 DO2 DO3 DO1E DO2E DO3E" # Seções separadas por espaço
tipo_dou = "DO1"

def brasilia_day():
    """
    No matter where the code is ran, return UTC-3 day
    (Brasilia local day, no daylight savings)
    """
    return (dt.datetime.utcnow() + dt.timedelta(hours=-3)).replace(hour=0, minute=0, second=0, microsecond=0)


def capture_DOU_driver(date, debug=False):

    # Inicialização do driver:
    driver = InLabsDriver()
    driver.login()
    
    # Montagem da URL:
    do_date_format = '%Y-%m-%d'
    # Transforms date to DOU format:
    date_string    = date.strftime(do_date_format)
    
    for dou_secao in tipo_dou.split(' '):
        file_url = driver.url_download + date_string + "&dl=" + date_string + "-" + dou_secao + ".zip"
        file_header = {'Cookie': 'inlabs_session_cookie=' + driver.cookie, 'origem': '736372697074'}
        file_response = driver.session.request("GET", file_url, headers = file_header)
        if file_response.status_code == 200:
            with open(f'../testing/local_out/{date_string}-{dou_secao}.zip', 'wb') as f:
                f.write(file_response.content)
                if debug:
                    print("File %s saved." % (date_string + "-" + dou_secao + ".zip"))
            del file_response
            del f
        elif file_response.status_code == 404:
                print("File not found: %s" % (date_string + "-" + dou_secao + ".zip"))
    exit(0)

if __name__ == "__main__":
    # Apenas para testagem
    capture_DOU_driver(brasilia_day())


