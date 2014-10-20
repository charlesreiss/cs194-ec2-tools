import boto
import random
import subprocess
import os
import os.path
import time

import sqlite3

POLL_DELAY = 10

SSH_OPTIONS = [
    '-o', 'ConnectTimeout=5',
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o', 'StrictHostKeyChecking=no',
]

def setup_args(parser):
    parser.add_argument('--creds_db', default='creds.db')
    parser.add_argument('--ssh_key_dir', default='ssh-keys')
    parser.add_argument('--password_wordlist', default='diceware_list.txt')
    parser.add_argument('--default_group', default='students')
    parser.add_argument('--aws_region', 'us-west-2')
    parser.add_argument('--instance_up_wait', default=120)
    parser.add_argument('--instance_stop_wait', default=180)
    parser.add_argument('--instance_pending_wait', default=600)
    parser.add_argument('--ssh_user_name', default='root')

def connect_db(args):
    return sqlite3.connect(args.creds_db, isolation_level=None)

def fill_dbh(args, dbh):
    if not dbh:
        return connect_db(args)
    else:
        return dbh

def init_db(args, dbh):
    dbh.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_name TEXT PRIMARY KEY,
            name TEXT,
            note TEXT,
            password TEXT
        );
    """)

def generate_password(args):
    with open(arg.password_words, 'r') as fh:
        words = map(lambda s: s.strip(), fh.readlines())
    rng = random.SystemRandom()
    return ' '.join([random.choice(words) for i in range(3)])

def get_all_users(args, dbh):
    c = dbh.cursor()
    c.execute("""
        SELECT user_name FROM users
    """)
    return c.fetchall()

def _generate_keypair(args, name):
    subprocess.check_call([
        'ssh-keygen', '-t', 'rsa', '-f', os.path.join(args.ssh_key_dir, name)
    ])
    with open(os.path.join(args.ssh_key_dir, name + '.pub'), 'r') as fh:
        return fh.read()

def _put_user_policy(args, iam, user_name):
    iam.put_user_policy(user_name,
        'StartStopTaggedInstances-' + user_name,
        """
        {
            "Version": "2012-10-17",
            "Statement":[{
                "Effect":"Allow",
                "Action":["ec2:StartInstances","ec2:StopInstances"],
                "Resource": "*",
                "Condition": {
                    "StringEquals": {
                        "ec2:ResourceTag/for_user": "{user_name}"
                    }
                }
            }]
       }
       """.format(user_name=user_name)
    )


def create_account(args, dbh, iam, ec2, user_name, name, note=''):
    response = iam.create_user(user_name)
    user = response.user
    password = generate_password(args)
    response = iam.create_login_profile(user_name, password)
    iam.add_user_to_group(user_name, password)
    public_key = _generate_keypair(args, user_name)
    _put_user_policy(args, iam, user_name)
    dbh.execute("""
        INSERT INTO users (user_name, name, note, password)
        VALUES (:user_name, :name, :note, :password)
    """, {'user_name': user_name, 'name': name, 'note': note, 'password': password})

def get_all_passwords(args, dbh, user_name):
    c = dbh.cursor()
    c.execute("""
        SELECT user_name, password FROM users ORDER BY user_name
    """, {'user_name': user_name})
    return c.fetchall()

def _find_active_instances_for(args, ec2, user_name):
    return ec2.get_all_instances(filters={
        'tag:for_user': user_name,
        'instance-state-name': [
            'pending', 'running', 'stopping', 'stopped',
        ]
    })

def instances_by_user(args, ec2):
    result = {}
    instance_tags = ec2.get_all_tags({'resource_type': 'instance', 'key': 'for_user'})
    for tag in instance_tags:
        user = tag.value
        instance = ec2.get_only_instances(instance_ids=[tag.res_id])[0]
        if instance.state != 'terminated' and instance.state != 'shutting-down':
            continue
        result.setdefault(user, []).append(instance)

def make_reservation_for(args, ec2, user_name, launch_args):
    launch_args['key_name'] = user_name
    reservation = ec2.run_instances(**launch_args)
    return reservation.instances

def healthcheck_instances(args, ec2, instances_by_user):
    pending_instances = instances_by_user.copy()
    failed_instances = {}
    stopping_since = {}
    running_since = {}
    while len(pending_instances) > 0:
        for user in pending_instances.keys():
            instances = pending_instances[user]
            still_pending = []
            for instance in instances:
                status = instance.update()
                now = time.time()
                did_fail = False
                did_succeed = False
                if status == 'stopping':
                    delay = now - stopping_since.setdefault(instance.id, now)
                    if delay > args.instance_stop_wait:
                        did_fail = True
                elif status == 'stopped':
                    did_succeed = True
                elif status == 'running' or status == 'rebooting':
                    delay = now - running_since.setdefault(instance.id, now)
                    is_up = False
                    try:
                        subprocess.check_call([
                            'ssh', '-i', os.path.join(args.ssh_key_dir, user),
                            '-l', args.ssh_user_name,
                        ] + SSH_OPTIONS + [
                            instance.public_ip_address,
                            '/bin/true'
                        ])
                        is_up = True
                    except subprocess.CalledProcessError:
                        pass
                    if is_up:
                        did_succeed = True
                    elif not is_up:
                        if delay > args.instance_up_wait:
                            did_fail = True
                else:
                    did_fail = True
                if did_fail:
                    failed_instances.setdefault(user, []).append(instance)
                elif not did_succeed:
                    still_pending.append(instance)
            if len(still_pending) > 0:
                pending_instances[user] = still_pending
            else:
                del pending_instances[user]
    return failed_instances

def wait_for_and_tag_instances(args, ec2, instances_by_user):
    pending_instances = instances_by_user.copy()
    for user in pending_instances:
        pending_instances[user] = pending_instances[user].copy()
    failed_instances = {}
    start_time = time.time()
    while len(pending_instances) > 0 and time.time() - start_time < args.pending_instance_wait:
        for user in pending_instances.keys():
            instances = pending_instances[user]
            still_pending = []
            for instance in instances:
                status = instance.update()
                if status == 'pending':
                    still_pending.append(instance)
                elif status == 'running':
                    instance.add_tag('for_user', user)
                else:
                    failed_instances.setdefault(user, []).append(instance)
            if len(still_pending) > 0:
                pending_instances[user] = still_pending
            else:
                del pending_instances[user]
        time.sleep(POLL_DELAY)
    for user, instances in pending_instances.iteritems():
        failed_instances.setdefault(user, []) += instances
    return failed_instances


