#!/usr/local/bin/python3.5

import csv
import sys
import logging
import asyncio
from aiohttp import ClientSession, Timeout
from boilerpipe.extract import Extractor
from connector import DbConnector

async def fetch(url, session):
    try:
        with Timeout(5):
            async with session.get(url) as response:
                assert response.status == 200
                content_type = response.headers.get('content-type', None)
                assert content_type is not None and 'text/html' in content_type
                return await response.read()
    except Exception:
        return None

async def run(urls, lat, lon):
    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    async with ClientSession() as session:
        for url in urls:
            task = asyncio.ensure_future(fetch(url, session))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        # you now have all response bodies in this variable
        for i, resp in enumerate(responses):
            if resp is not None and len(resp) > 300:
                try:
                    extractor = Extractor(
                        extractor='DefaultExtractor', html=resp)
                    extracted_text = extractor.getText()
                    if len(extracted_text) > 150:
                        args = (lat, lon, urls[i], extracted_text)
                        db.add(args)
                except Exception as error:
                    logging.info('fail_' + str(urls[i]))


def main():
    file_name = sys.argv[1]
    number_line = sys.argv[2]
    csv.field_size_limit(sys.maxsize)
    log_path = file_name + ".log"
    logging.basicConfig(filename=log_path, level=logging.INFO)
    logging.info('init blacklist')

    blacklist = []
    with open('blacklist.txt', 'r') as myfile:
        data_file = myfile.read()
        blacklist = data_file.split('\n')
        blacklist.pop()

    with open(file_name, "r") as csvfile:
        reader = csv.DictReader(csvfile, ['lat', 'long', 'urls'])

        for n, row in enumerate(reader):

            # skip lines
            if n < int(number_line):
                continue

            logging.info("number: %s", n)
            row['urls'] = row['urls'][1:-1].split('|')

            # remove blacklisted url
            temp = [item for item in row['urls'] 
                if not any(x in item for x in blacklist)]

            # parse urls
            if len(temp) > 0:
                loop = asyncio.get_event_loop()
                future = asyncio.ensure_future(
                    run(temp, row['lat'], row['long']))
                loop.run_until_complete(future)
                db.commit()
    db.close()                

if __name__ == '__main__':
    db = DbConnector(sys.argv[1])
    main()
