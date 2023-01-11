import datetime as dt
from inlabs_driver import InLabsDriver

tipo_dou="DO1 DO2 DO3 DO1E DO2E DO3E" # Seções separadas por espaço

def brasilia_day():
    """
    No matter where the code is ran, return UTC-3 day
    (Brasilia local day, no daylight savings)
    """
    return (dt.datetime.utcnow() + dt.timedelta(hours=-3)).replace(hour=0, minute=0, second=0, microsecond=0)


def download(date, debug=False):

    # Inicialização do driver:
    driver = InLabsDriver()
    driver.login()

    if driver.session.cookies.get('inlabs_session_cookie'):
        cookie = driver.session.cookies.get('inlabs_session_cookie')
    else:
        print("Failed to obtain cookies. Verify credentials")
        exit(37)
    
    # Montagem da URL:
    do_date_format = '%d-%m-%Y'
    # Transforms date to DOU format:
    date_string    = date.strftime(do_date_format)
    
    for dou_secao in tipo_dou.split(' '):
        file_url = driver.url_download + date_string + "&dl=" + date_string + "-" + dou_secao + ".zip"
        file_header = {'Cookie': 'inlabs_session_cookie=' + cookie, 'origem': '736372697074'}
        file_response = driver.session.request("GET", file_url, headers = file_header)
        if file_response.status_code == 200:
            with open(f'output\{date_string}-{dou_secao}', 'wb') as f:
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
    download(brasilia_day())


