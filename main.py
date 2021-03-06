# encoding: utf-8

import config

from torndb import Connection
import requests
import json
from time import sleep

db = Connection(config.mysql['host'], config.mysql['database'], user=config.mysql['user'], password=config.mysql['password'])

states = {
    '2012': {'label': u":speech_balloon: Initialisation", 'color': '', 'post': False},
    '2021': {'label': u":white_check_mark: OK", 'color': 'good', 'post': True},
    '2032': {'label': u":zzz: Sleep", 'color': '', 'post': True},
    '2042': {'label': u":inbox_tray: Plus de papier", 'color': 'warning', 'post': True},
    '2043': {'label': u":japanese_goblin: Bourrage papier", 'color': 'danger', 'post': True},
    '2044': {'label': u":printer: Imprimante déconnectée", 'color': 'danger', 'post': True},
    '3012': {'label': u":speech_balloon: Initialisation (mode test)", 'color': '', 'post': True},
    '3021': {'label': u":white_check_mark: OK (mode test)", 'color': 'good', 'post': True},
    '3032': {'label': u":zzz: Sleep (mode test)", 'color': '', 'post': True},
    '3042': {'label': u":inbox_tray: Plus de papier (mode test)", 'color': 'warning', 'post': True},
    '3043': {'label': u":japanese_goblin: Bourrage papier (mode test)", 'color': 'danger', 'post': True},
    '3044': {'label': u":printer: Imprimante déconnectée (mode test)", 'color': 'danger', 'post': True},
}

def get_new_rows(last_date):
    sql = """
        SELECT s.id, d.id AS d_id, s.occured_at, s.state, d.short_name, d.name AS d_name, c.name, d.mode
        FROM dispenser_log_status s
        LEFT JOIN dispensers d ON s.dispenser_id = d.id
        LEFT JOIN dispenser_customers c ON d.customer_id = c.id
        WHERE s.occured_at > %s
        ORDER BY s.occured_at ASC
    """

    return db.query(sql, last_date)

def post_message(data):
    requests.post(config.slack['hook_url'], data=json.dumps(data))

def post_error(msg):
    print "[error] ", msg
    post_message({
      'username': "Erreur",
      'text': msg,
      'icon_emoji': ':dhc:',
      'channel': '#statuts',
    })

def handle_new_row(row, channel='#statuts'):
    name = row['name'] if 'name' in row else "<name>"
    d_name = row['d_name'] if 'd_name' in row else "<d_name>"
    d_id = row['d_id'] if 'd_id' in row else "<d_id>"
    state = states[row['state']]['label'] if 'state' in row and row['state'] in states else "<state>"
    color = states[row['state']]['color'] if 'state' in row and row['state'] in states else ""
    
    if 'mode' in row and row['mode'] in ['preprod', 'dev']:
        channel = '#statuts-preprod'

    if 'state' in row and row['state'] in states and not states[row['state']]['post']:
        return

    post_message({
      'username': name,
      'icon_emoji': ':dhc:',
      'channel': channel,
      'attachments': [
          {
              "author_name": d_name,
              "author_link": "http://short-edition.com/admin/distributeur/%s/show" % d_id,
              "text": state,
              "color": color,
          }
      ]
    })

def get_last_date():
    return db.get("SELECT occured_at FROM dispenser_log_status ORDER BY occured_at DESC LIMIT 1")['occured_at']

def infinite_loop():
    last_date = get_last_date()
    while True:
        try:
            print "checking"
            rows = get_new_rows(last_date)
            if rows:
                for row in rows:
                    try:
                        handle_new_row(row)
                    except Exception, e:
                        post_error(str(e) + '\n' + str(row))
                    
                last_date = rows[-1]['occured_at']
        except Exception, e:
            post_error(str(e))
            sleep(60)
        
        sleep(3)


def test_post():
    last_date = get_last_date()
    print last_date
    rows = get_new_rows("2016-05-17 16:50:01")
    for row in rows:
        try:
            handle_new_row(row, '#tmp')
        except Exception, e:
            post_error(str(e) + '\n' + str(row))

if __name__ == '__main__':
    infinite_loop()

    # test_post()

