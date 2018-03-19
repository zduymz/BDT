import pykafka
import config
import json
import csv
import os
from os.path import isfile, join
import time


def csv_to_kfk(filename):
    with open(filename, 'rU') as f:
        try:
            csvReader = csv.reader(f, dialect=csv.excel_tab)
        except Exception as e:
            return
        next(csvReader, None)

        for line in csvReader:
            send_data = '{}'
            json_send_data = json.loads(send_data)
            try:
                json_send_data['id'] = line[3]
                json_send_data['name'] = line[4]
                json_send_data['location'] = line[2]
                json_send_data['text'] = line[0]
                json_send_data['senti_val'] = line[5]
            except Exception as e:
                continue
            producer.produce(bytes(json.dumps(json_send_data)))
            print(json_send_data)

if __name__ == "__main__":

    client = pykafka.KafkaClient(config.KAKFA_BOOTSTRAP_SERVER)
    producer = client.topics['twitter'].get_producer()

    fpath = "output"
    csvFiles = [f for f in os.listdir(fpath) if isfile(
        join(fpath, f)) and join(fpath, f).lower().endswith(".csv")]
    for file in csvFiles:
        csv_to_kfk(join(fpath, file))
