import json

import pandas as pd
from sqlalchemy import create_engine


class SqlClient:
    def __init__(self, host, port, username, password, db):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db = db
        self._conn = None
        self.init_conn()

    def init_conn(self):
        _conn_string = f'mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.db}?charset=utf8'
        self._conn = create_engine(_conn_string)

    def query(self, query):
        df = pd.read_sql(query, self._conn)
        if not df.empty:
            data_dict = df.set_index(df.columns[0]).T.to_dict()
            return data_dict
        return {}


if __name__ == '__main__':
    sql_client = SqlClient('ip', 'port', 'username', 'password', 'data_region')

    country_query = 'SELECT * From data_region where level=2'

    country_map = {}

    country_data = sql_client.query(country_query)
    for country_id, country_detail in country_data.items():
        states = []
        country_data = {'label': country_detail['name'], 'value': country_detail['name'],
                        'label_en': country_detail['name_en'], 'children': states}
        country_map[country_detail['name_en'].replace(u'\xa0', u' ')] = country_data
        state_or_province_query = 'SELECT * From data_region where level=3 and pid=%s' % country_id
        state_or_province_data = sql_client.query(state_or_province_query)
        for state_or_province_id, state_or_province_detail in state_or_province_data.items():
            city_query = 'SELECT * From data_region where level=4 and pid=%s' % state_or_province_id
            city_data = sql_client.query(city_query)
            states.append({'label': state_or_province_detail['name'], 'value': state_or_province_detail['name'],
                           'label_en': state_or_province_detail['name_en'],
                           'children': [i['name'] for i in city_data.values()],
                           'children_en': [i['name_en'] for i in city_data.values()],
                           })

    with open('data_region.json', mode='w', encoding='utf8') as f:
        f.write(json.dumps(country_map, ensure_ascii=False))
