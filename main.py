# encoding: utf-8

import config

from torndb import Connection
import requests
import json
from time import sleep

db = Connection(config.mysql['host'], config.mysql['database'], user=config.mysql['user'], password=config.mysql['password'])

states = {
    '2012': {'label': ":speech_balloon: Initialisation"},
    '2021': {'label': ":white_check_mark: OK"},
    '2032': {'label': ":zzz: Sleep"},
    '2042': {'label': ":inbox_tray: Plus de papier"},
    '2043': {'label': ":japanese_goblin: Bourrage papier"},
    '2044': {'label': ":printer: Imprimante déconnectée"},
    '3012': {'label': ":speech_balloon: Initialisation (mode test)"},
    '3021': {'label': ":white_check_mark: OK (mode test)"},
    '3032': {'label': ":zzz: Sleep (mode test)"},
    '3042': {'label': ":inbox_tray: Plus de papier (mode test)"},
    '3043': {'label': ":japanese_goblin: Bourrage papier (mode test)"},
    '3044': {'label': ":printer: Imprimante déconnectée (mode test)"},
}

def get_new_rows(last_id):
    sql = """
        SELECT s.id, d.id AS d_id, s.created_at, s.state, d.short_name, d.name AS d_name, c.name, d.mode
        FROM dispenser_log_status s
        LEFT JOIN dispensers d ON s.dispenser_id = d.id
        LEFT JOIN dispenser_customers c ON d.customer_id = c.id
        WHERE s.id > %s
        ORDER BY id ASC
    """

    return db.query(sql, last_id)

def post_message(data):
    requests.post(config.slack['hook_url'], data=json.dumps(data))

def post_error(msg):
    post_message({
      'username': "Erreur",
      'text': msg,
      'icon_emoji': ':dhc:',
      'channel': '#statuts-preprod' if 'mode' in row and row['mode'] == 'preprod' else '#statuts',
    })

def handle_new_row(row):
    name = row['name'] if 'name' in row else "<name>"
    d_name = row['d_name'] if 'd_name' in row else "<d_name>"
    d_id = row['d_name'] if 'd_id' in row else "<d_id>"
    state = states[row['state']]['label'] if 'state' in row and row['state'] in states else "<state>"
    channel = '#statuts-preprod' if 'mode' in row and row['mode'] == 'preprod' else '#statuts'


    post_message({
      'username': "%s (%s)" % (name, d_name),
      'text': "<http://short-edition.com/admin/distributeur/%s/show|%s>" % (d_id, state),
      'icon_emoji': ':dhc:',
      'channel': channel,
    })

last_id = db.get("SELECT id FROM dispenser_log_status ORDER BY id DESC LIMIT 1")['id']
while True:
    try:
        rows = get_new_rows(last_id)
        if rows:
            for row in rows:
                handle_new_row(row)
                
            last_id = rows[-1]['id']
    except Exception, e:
        post_error(e.message)
        sleep(60)
    
    sleep(3)