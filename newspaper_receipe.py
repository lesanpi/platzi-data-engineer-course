import argparse
import logging
logging.basicConfig(level=logging.INFO)
from urllib.parse import urlparse
import pandas as pd
import hashlib

logger = logging.getLogger(__name__)

def main(filename):
    logger.info('Starting cleaning process')

    df = _read_data(filename)
    newspaper_uid = _extract_newspaper_uid(filename)
    df = _add_newspaper_uid_column(df, newspaper_uid)
    df = _extract_host(df)
    df = _fill_missing_titles(df)
    df = _generate_uids_for_rows(df)
    df = _remove_new_lines_from_body(df)

    return df

def _read_data(filename):
    logger.info(f'Reading file {filename}')
    return pd.read_csv(filename)

def _extract_newspaper_uid(filename):
    logger.info("Extracting the newspaper uid")
    newspaper_uid = filename.split('_')[0]

    logger.info(f"Newspaper uid detected: {newspaper_uid}")
    return newspaper_uid

def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info(f"Filling newspaper uid column with {newspaper_uid}")
    df['newspaper_uid'] = newspaper_uid

    return df

def _extract_host(df):
    logger.info("Extracting the host from urls")
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    return df

def _fill_missing_titles(df: pd.DataFrame):
    logger.info("Filling missing titles")
    missing_titles_mask = df['title'].isna()

    missing_titles = (df[missing_titles_mask]['url'].str.extract('r(?P<missing_titles>[^/]+)$')
                        .applymap(lambda title: title.split('-'))
                        .applymap(lambda title: ' '.join(title))
                        )

    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']
    return df

def _generate_uids_for_rows(df: pd.DataFrame):
    logger.info("Generating uids for each row")
    uids = (df
            .apply(lambda row: hashlib.hashlib.md5(bytes(row['url'].encode())), axis = 1)
            .apply(lambda hash_obj: hash_obj.hexdigest())
    )
    df['uid'] = uids
    return df.set_index('uid')

def _remove_new_lines_from_body(df: pd.DataFrame):
    logger.info("Removing new lines from body")
    stripped_body = (df
                        .apply(lambda row: row['body'].replace('\n', ''), axis=1)
                    )
    df['body'] = stripped_body
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The path to the dirty data',
                        type=str)
    arg = parser.parse_args()
    df = main(arg.filename)

    print(df)