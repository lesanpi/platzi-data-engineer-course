import logging
logging.basicConfig(level=logging.INFO)
import subprocess

logger = logging.getLogger(__name__)
news_sites_uids = ['eluniversal', 'elpais']

def main():
    _extract()
    _transform()
    _load()


def _extract():
    logger.info('Starting extract process')
    for news_site_uid in news_sites_uids:
        subprocess.run(['python', 'main.py', news_sites_uids], cwd='./extract')
        subprocess.run(['find', '.', '-name', f'{news_site_uid}*',
                    '-exec', 'mv', '{}', f'../transform/{news_site_uid}_.csv'],
                    ';', cwd='./extract')
    
def _transform():
    logger.info('Starting transform process')
    for news_site_uid in news_sites_uids:
        dirty_data_filename = f'{news_site_uid}_.csv'
        clean_data_filename = f'clean_{news_site_uid}.csv'
        subprocess.run(['python', 'main.py', dirty_data_filename], cwd='./transform')
        subprocess.run(['rm', dirty_data_filename], cwd='./transform')
        subprocess.run(['mv', clean_data_filename, f'../load/{news_site_uid}.csv'], cwd='./transform')

def _load():
    logger.info('Starting load process')
    for news_site_uid in news_sites_uids:
        clean_data_filename = f'{news_site_uid}.csv'
        subprocess.run(['python', 'main.py', clean_data_filename], cwd='./load')
        subprocess.run(['rm', clean_data_filename], cwd='./load')

if __name__ == '__main__':
    main()