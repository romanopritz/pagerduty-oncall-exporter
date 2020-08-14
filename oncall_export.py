#!/usr/bin/env python

from __future__ import print_function
import argparse
import yaml
import sys
import pytz
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from lxml import html
from tabulate import tabulate
from datetime import datetime, timedelta


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=str, help="configuration file")
    args = parser.parse_args()

    return args


def read_config():
    with open(args.config, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    cfg['mobileworx']['login_url'] = (cfg['mobileworx']['site_url']
        + '/apps/ServicePortal/public/login.php?lang=de')

    cfg['mobileworx']['upload_url'] = (cfg['mobileworx']['site_url']
        + '/apps/ServicePortal/public/sites/holidaycheck/upload.php')

    return cfg


def utc_to_local(utc_dt):
    local_tz = pytz.timezone(cfg['pagerduty']['timezone'])
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)

    return local_tz.normalize(local_dt)


def https_session():
    retries = Retry(total=5, backoff_factor=0.2, 
                    status_forcelist=[ 500, 502, 503, 504 ])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))

    return session


def pagerduty_api_request(module, payload={}):
    session = https_session()
    data = []
    reply = {'offset': 0, 'more': True, 'limit': 25}
    url = '{}/{}'.format(cfg['pagerduty']['api_url'], module)
    headers = {
        'Accept': 'application/vnd.pagerduty+json;version=2',
        'Authorization': 'Token token={token}'.format(
            token=cfg['pagerduty']['api_key'])
        }   

    # Fetch paginated content
    while reply['more'] == True:
        response = session.get(url, headers=headers, params=payload)
        
        if response.status_code != 200:
            raise RuntimeError('PagerDuty API request error:\n'
                                            + repr(response.text))
        reply = response.json()
        payload.update({'offset': reply['offset'] + reply['limit']-1})
        data += reply[module]
    
    return data


def get_users():
    payload = {'include[]': ['contact_methods']}
    users = pagerduty_api_request('users', payload)
    users_dict = dict((item['id'], item) for item in users)

    return users_dict


def get_phone(users_dict, user_id):
    phone_contact = next((item 
            for item in users_dict[user_id]['contact_methods'] 
            if item['type'] == 'phone_contact_method'), None)

    phone_number = '00{!s}{!s}'.format(
            phone_contact['country_code'], 
            phone_contact['address'])

    return phone_number


def get_username(users_dict, user_id):
    return(users_dict[user_id]['summary'])


def get_oncalls():
    until_time = (datetime.now() 
        + timedelta(days=cfg['pagerduty']['range_days'])
                            ).strftime('%Y-%m-%d %H:%M:%S')
    payload = {
        'time_zone': 'UTC',
        'schedule_ids[]': cfg['pagerduty']['schedule_ids'],
        'until': until_time
        }

    oncalls_list = pagerduty_api_request('oncalls', payload)

    return oncalls_list


def oncalls_to_timetable(oncalls_list):
    timetable_dict = {}
    roles_map = {
            1: 'primary',
            2: 'secondary'
            }

    # Load oncall events into 'timetable_dict':
    #           start/end timestamp -> primary: user_id
    #                               -> secondary: user_id
    for oncall in oncalls_list:
        role = roles_map.get(oncall['escalation_level'], None)
        
        if role in ('primary', 'secondary'):
            user_id = oncall['user']['id']

            timetable_dict.setdefault(
                    oncall['start'], {}).update({role: user_id})

            timetable_dict.setdefault(
                    oncall['end'], {}).update({role: None})

    return timetable_dict


def timetable_normalize(timetable_dict):
    timetable_list = []
    cnt = 0

    for item in sorted(timetable_dict):
        timetable_list.append({item: timetable_dict[item]})
        prev_item = next((x for x in timetable_list[cnt-1].keys()), None)
        
        # If no primary contact for the timestamp, copy from previous record
        if not 'primary' in timetable_list[cnt][item].keys():
            timetable_list[cnt][item]['primary'] = \
                    timetable_list[cnt-1][prev_item]['primary']
        
        # If no secondary contact for the timestamp, copy from previous record
        if not 'secondary' in timetable_list[cnt][item].keys():
            timetable_list[cnt][item]['secondary'] = \
                    timetable_list[cnt-1][prev_item]['secondary']

        cnt += 1

    return timetable_list


def create_schedule(oncalls_list, users_dict):
    schedule_list = []
    timetable_dict = oncalls_to_timetable(oncalls_list)
    timetable_list = timetable_normalize(timetable_dict)
    
    for i in range(len(timetable_list) - 1):
        start = next((x for x in timetable_list[i].keys()), None)
        end = next((x for x in timetable_list[i+1].keys()), None)
        primary = timetable_list[i][start]['primary']
        secondary = timetable_list[i][start]['secondary']

        # Skip rows with no oncall person
        if primary is None and secondary is None:
            continue

        # Convert UTC to local time
        start_local = (utc_to_local(datetime.strptime(start, 
                '%Y-%m-%dT%H:%M:%SZ'))).strftime('%Y-%m-%d %H:%M:%S')
        end_local = (utc_to_local(datetime.strptime(end, '%Y-%m-%dT%H:%M:%SZ')
                - timedelta(seconds=1))).strftime('%Y-%m-%d %H:%M:%S')

        schedule_list.append([
                start_local,
                end_local,
                get_phone(users_dict, primary),
                get_username(users_dict, primary),
                get_phone(users_dict, secondary),
                get_username(users_dict, secondary)
                ])

    return schedule_list


def print_schedule(schedule_list):
    headers = [
        'Start', 'End', 
        'Primary phone',
        'Primary contact', 
        'Secondary phone', 
        'Secondary contact'
        ]
    print(tabulate(schedule_list,
            headers=headers, 
            tablefmt='grid', 
            numalign='left'))


def save_schedule(schedule_list):
    with open(cfg['csv_file'], 'w') as csvfile:
        csvfile.write('StartDatum;EndeDatum;Handy1;Handy2\n')
        for i in schedule_list:
            line = '"{}";"{}";{};{}\n'.format(i[0], i[1], i[2], i[4])
            csvfile.write(line)


def mobileworx_login():
    session = https_session()
    payload = {
        'username': cfg['mobileworx']['login'], 
        'password': cfg['mobileworx']['password']
    }
    
    login = session.post(cfg['mobileworx']['login_url'], data=payload)

    if 'eingeloggt als: ' + cfg['mobileworx']['login'] in login.text:
        print('Mobileworx login successful')
        return session
    else:
        raise RuntimeError('Mobileworx login failed:\n' + repr(login.text))


def mobileworx_upload(session):
    update = session.get(cfg['mobileworx']['upload_url'])
    update_html = html.fromstring(update.content)
    token = update_html.forms[0].inputs['token'].value
    action = update_html.forms[0].action
    payload = {
        'token': token, 
        'service': str(cfg['mobileworx']['service_id']),
        'submit': 'OK'
    }

    with open(cfg['csv_file'], 'rb') as f:
        upload = session.post(cfg['mobileworx']['upload_url'] + action,
                                data=payload, files={'userfile': f})
        if upload.status_code == 200:
            print('Mobileworx upload successful')
        else:
            raise RuntimeError('Mobileworx upload failed:\n' + repr(upload.text))

    return html.fromstring(upload.content)


def mobileworx_verify(session, upload_html):
    # Download the file and compare to the original
    check_url = (cfg['mobileworx']['site_url'] + upload_html.xpath(
                    '//*[@id="table_1"]/tfoot/tr[2]/td/a[1]/@href')[0])

    check = session.get(check_url)
    if check.status_code != 200:
        raise RuntimeError('Mobileworx verification failed:\n' + repr(check.text))

    check_file = sorted(list(filter(None, check.text.split('\n'))))

    with open(cfg['csv_file'], 'r') as f:
        orig_file = sorted(list(filter(None, f.read().split('\n'))))

    if check_file == orig_file:
        print('Mobileworx verification successful')
    else:
        raise RuntimeError('Mobileworx verification failed: files do not match')


def mobileworx_save(session, upload_html):
    action = upload_html.forms[0].action
    payload = {
        'service': str(cfg['mobileworx']['service_id']),
        'save': 'OK'
    }
    
    save = session.post(
        cfg['mobileworx']['upload_url'] + action, data=payload)
    
    if save.status_code != 200:
        raise RuntimeError('Mobileworx save error:\n' + repr(save.text))

    save_html = html.fromstring(save.content)
    save_status = save_html.xpath('//*[@id="content"]/div/h1/text()')[0]

    if save_status == 'Liste':
        print('Mobileworx save successful')
    else:
        raise RuntimeError('Mobileworx save failed:\n' + repr(save.text))


def upload_schedule():
    session = mobileworx_login()
    upload_result = mobileworx_upload(session)
    mobileworx_verify(session, upload_result)
    mobileworx_save(session, upload_result)


def main():
    global args, cfg
    args = parse_args()
    cfg = read_config()

    try:
        oncalls = get_oncalls()
        users = get_users()
        schedule = create_schedule(oncalls, users)
        print_schedule(schedule)
        save_schedule(schedule)
        upload_schedule()

    except RuntimeError as err:
        eprint(err.message)
        return 1

    return 0

if __name__ == '__main__':
    sys.exit(main())

