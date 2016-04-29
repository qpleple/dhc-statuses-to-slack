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

def handle_new_row(row):
    post_message({
      'username': "%s (%s)" % (row['name'], row['d_name']),
      'text': "<http://short-edition.com/admin/distributeur/%s/show|%s>" % (row['d_id'], states[row['state']]['label']),
      'icon_emoji': ':dhc:',
      'channel': '#statuts' if row['mode'] == 'prod' else '#statuts-preprod',
    })

last_id = db.get("SELECT id FROM dispenser_log_status ORDER BY id DESC LIMIT 1")['id']
while True:
    rows = get_new_rows(last_id)
    if rows:
        for row in rows:
            handle_new_row(row)
        last_id = rows[-1]['id']
    
    sleep(3)