import datetime as dt
from inlabs_driver import InLabsDriver
# This project's functions:
import global_settings as gs
import get_articles_url as gu
import parse_dou_article as pa
import write_article as wa
import filter_articles as fa
import structure_article as sa
import post_to_slack as ps
import configure_iteration as ci

def brasilia_day():
    """
    No matter where the code is ran, return UTC-3 day
    (Brasilia local day, no daylight savings)
    """
    return (dt.datetime.utcnow() + dt.timedelta(hours=-3)).replace(hour=0, minute=0, second=0, microsecond=0)

def captured_article_ok(save_option, saved, post_option, posted):
    """
    Given four boolean variables, return whether or not the article 
    should be considered captured or not.
    
    save_option: Was the code required to save the article?
    saved:       Did the code save the article?
    post_option: Was the code required to post the article?
    posted:      Did the code post the article?
    """
    # Nothing to do with article, mark as done.
    if save_option == False and post_option == False:
        return True
    # Only requested saving and it saved:
    if saved == True and post_option == False:
        return True
    # Only requested posting and it posted:
    if posted == True and save_option == False:
        return True
    # Did both saving and posting:
    if saved == True and posted == True:
        return True

    return False

def capture_DOU_driver(event):
    """
    This is the driver that runs DOU articles' capture.
    It receives either a filename (string) for a configuration file 
    or a configuration as a dict with keywords:

    * save_articles: BOOL that tells whether or not to write all articles to database; 
    * date_format:   the format of end_date (e.g. %Y-%m-%d);
    * end_date:      the last day to search for articles (cat be set to 'now');
    * secao:         a list with DOU sections to search immediately;
    * secao_all:     a list with all DOU sections that one is interested in searching in the future
                     (1, 2, 3, 'e', '1a'; only relevant for scheduler); 
    * timedelta:     the number of days in the pasr from end_date to start the search
                     (a negative number);
    * filter_file:   JSON filename that describes the filters to be applied to articles;
    * post_articles: BOOL that tells whether or not to post articles to Slack;
    * slack_token:   Filename for file containing Slack's authentication token.

    It returns an updated configuration file for the next capture (assuming one wants 
    to periodically capture the DOU publications.
    """    

    # Load configuration:
    if type(event) == str:
        config = gu.load_local_config(event)
    elif type(event) == dict:
        config = event
    else:
        raise Exception('capture_DOU_driver: unknown input event type.')   
  
    skip, next_config = ci.configure_iteration(config)
    if skip:
        return next_config
  
    # Load filters:
    if gs.debug:
        print("Loading filters...")    
    if gs.local:
        filters = fa.load_local_filters(config['filter_file'])
    else:
        filters = fa.load_remote_filters()
    bot_infos = fa.format_filters(filters)

    # Remove filters (that need to exist to select articles) that eliminate all downloaded sections:
    if gs.debug:
        Nfilters = len(bot_infos)
        print("Removing unecessary filters...")
    bot_infos = list(filter(lambda bot_info: len(fa.secao_left(config['secao'], bot_info))>0, bot_infos))
    if gs.debug:
        print("Removed " + str(Nfilters - len(bot_infos)) + " filters.")

    # The lists inside relevant_articles will receive the articles selected by each filter set:
    relevant_articles = [[]]*len(bot_infos)

    # Inicialização do driver:
    driver = InLabsDriver()
    driver.login()
    
    # Transforms date to DOU format:
    date_string    = config['end_date']
    
    for dou_secao in next_config['secao']:
        file_url = driver.url_download + date_string + "&dl=" + date_string + "-" + dou_secao + ".zip"
        file_header = {'Cookie': 'inlabs_session_cookie=' + driver.cookie, 'origem': '736372697074'}
        file_response = driver.session.request("GET", file_url, headers = file_header)
        if file_response.status_code == 200:
            for xml_root, filename in pa.load_zipped_response(file_response):
                
                raw_article = pa.parse_dou_article(xml_root)
                article = sa.structure_article(raw_article)

                wrote_return = 2
                if config['save_articles']:
                    if gs.debug:
                        print("Saving article...")
                    if gs.local:
                        wa.write_local_article(config, raw_article, '')
                        wrote_return = 200
                    else:
                        stuctured_filename = wa.build_filename(config['end_date'], dou_secao, filename, hive_partitioning=True)
                        if gs.debug:
                            print('Writing to S3: ' + stuctured_filename)
                        write_return = wa.write_to_s3(config, raw_article, stuctured_filename)
                        if write_return == 200:
                            wrote_return = wa.copy_s3_to_storage_gcp(config['bucket'], config['key'] + stuctured_filename)
                            if wrote_return != 200 and gs.debug:
                                raise Exception('Copy_s3_to_storage_gcp failed.') 
                        elif gs.debug:
                            print('Write_to_s3 failed.')

                # Loop over filters:
                if gs.debug:
                    print("Filtering article...")
                for i in range(len(bot_infos)):
                    # Filter article:
                    relevant_articles[i] = relevant_articles[i] + fa.get_relevant_articles(bot_infos[i], [article])
                    # Slack crashes if message has more than 50 blocks.
                    # Avoid this by pre-posting long messages:
                    if config['post_articles'] and len(relevant_articles[i]) > 20:
                        if gs.debug:
                            print('Selected more than 20 articles.')
                        ps.post_article(config, bot_infos[i], relevant_articles[i])
                        relevant_articles[i] = []

                # Record URL in list of captured articles (for now, we will assume that the article always was posted):
                if captured_article_ok(config['save_articles'], wrote_return==200, config['post_articles'], True):
                    gu.register_captured_url(config['url_list'], filename)
                elif gs.debug:
                    print('Failed to record as done: ' + filename)
        
        elif file_response.status_code == 404:
            # GET ran but returned 404:
            print('No DOU for ' + date_string + ' and section ' + dou_secao)          
            
        else:
            # GET ran but returned BAD STATUS:
            raise Exception('Bad status in GET', file_response.status_code)

    if config['post_articles']:
        # Send the selected articles to Slack:
        for i in range(len(bot_infos)):
            if len(relevant_articles[i]) > 0:
                ps.post_article(config, bot_infos[i], relevant_articles[i])

    # Return the config for next capture try:
    return next_config

if __name__ == "__main__":
    # Apenas para testagem
    capture_DOU_driver(brasilia_day())


