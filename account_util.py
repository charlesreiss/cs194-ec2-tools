import boto
import boto.ec2
import logging
import random
import subprocess
import os
import os.path
import time

import sqlite3

LOGGER = logging.getLogger(__name__)

POLL_DELAY = 10

SSH_OPTIONS = [
    '-o', 'ConnectTimeout=5',
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o', 'StrictHostKeyChecking=no',
]

def setup_args(parser):
    group = parser.add_argument_group('account_util')
    group.add_argument('--aws_region', default='us-west-2')
    group.add_argument('--profile', default=os.environ.get('AWS_PROFILE'))
    group.add_argument('--creds_db', default='creds.db')
    group.add_argument('--ssh_key_dir', default='ssh-keys')
    group.add_argument('--password_wordlist', default='diceware_list.txt')
    group.add_argument('--default_group', default='students')
    group.add_argument('--default_security_group', default='students')
    group.add_argument('--instance_up_wait', default=120)
    group.add_argument('--instance_stop_wait', default=180)
    group.add_argument('--instance_pending_wait', default=600)
    group.add_argument('--ssh_user_name', default='ec2-user')

    group.add_argument('--boto_log_level', choices=['DEBUG','INFO','WARNING','ERROR', 'CRITICAL'], default='INFO')
    group.add_argument('--account_util_log_level', choices=['DEBUG','INFO','WARNING','ERROR', 'CRITICAL'], default='DEBUG')

    # default is all users
    parser.add_argument('--users', default=None)
    parser.add_argument('--users_from_list', default=None)

    parser.add_argument('--ssh_keygen', default='ssh-keygen')

def get_users(args):
    if args.users_from_list:
        assert(not args.users)
        user_list = []
        with codecs.open(args.users_from_list, 'r', 'utf-8') as fh:
            for line in fh:
                user_list.append(line.split('\t')[1])
    elif args.users:
        user_list = args.users.split(',')
    else:
        dbh = connect_db(args)
        user_list = account_util.get_all_users(args, dbh)
        dbh.close()
    return user_list

def init_logging(args):
    logging.basicConfig()
    LOGGER.setLevel(logging.__dict__[args.account_util_log_level])
    logging.getLogger('boto').setLevel(logging.__dict__[args.boto_log_level])

def connect_ec2(args):
    return boto.ec2.connect_to_region(args.aws_region, profile_name=args.profile)

def connect_iam(args):
    return boto.connect_iam(profile_name=args.profile)

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
    with open(args.password_wordlist, 'r') as fh:
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
        args.ssh_keygen, '-N', '', '-t', 'rsa', '-f', os.path.join(args.ssh_key_dir, name),
        '-C', name
    ])
    with open(os.path.join(args.ssh_key_dir, name + '.pub'), 'r') as fh:
        return fh.read()

def _put_user_policy(args, iam, user_name):
    policy = \
        """
        {
            "Version":"2012-10-17",
            "Statement": [
                {
                    "Sid":"StartStopInstances%(user_name)s",
                    "Effect": "Allow",
                    "Action": [
                        "ec2:StartInstances",
                        "ec2:StopInstances",
                        "ec2:RebootInstances"
                    ],
                    "Condition": {
                        "StringEquals": {
                            "ec2:ResourceTag/for_user": "%(user_name)s"
                        }
                    },
                    "Resource": [
                        "*"
                    ]
                }
            ]
       }
       """ % {'user_name': user_name}
    policy = policy.strip()
    LOGGER.debug('policy is %s', policy)
    iam.put_user_policy(user_name,
        'StartStopTaggedInstances-' + user_name,
        policy,
    )


def wipe_account(args, dbh, iam, ec2, user_name):
    try:
        iam.remove_user_from_group(args.default_group, user_name) 
    except boto.exception.BotoServerError:
        pass

    try:
        iam.delete_login_profile(user_name) 
    except boto.exception.BotoServerError:
        pass

    try:
        iam.delete_user_policy(user_name, 'StartStopTaggedInstances-' + user_name)
    except boto.exception.BotoServerError:
        pass
    try:
        ec2.delete_key_pair(user_name)
        if os.path.exists(os.path.join(args.ssh_key_dir, user_name)):
            os.unlink(os.path.join(args.ssh_key_dir, user_name))
            os.unlink(os.path.join(args.ssh_key_dir, user_name + '.pub'))
    except boto.exception.BotoServerError:
        pass

    try:
        iam.delete_user(user_name) 
    except boto.exception.BotoServerError:
        pass

    dbh.execute("""
        DELETE FROM users WHERE user_name = :user_name
    """, {'user_name': user_name})

def create_account(args, dbh, iam, ec2, user_name, name, note=''):
    response = iam.create_user(user_name)
    user = response.user
    password = generate_password(args)
    response = iam.create_login_profile(user_name, password)
    iam.add_user_to_group(args.default_group, user_name)
    public_key = _generate_keypair(args, user_name)
    _put_user_policy(args, iam, user_name)
    ec2.import_key_pair(user_name, public_key)
    dbh.execute("""
        INSERT INTO users (user_name, name, note, password)
        VALUES (:user_name, :name, :note, :password)
    """, {'user_name': user_name, 'name': name, 'note': note, 'password': password})

def get_all_passwords(args, dbh):
    c = dbh.cursor()
    c.execute("""
        SELECT user_name, password FROM users
    """)
    return dict(c.fetchall())

def get_all_users(args, dbh):
    c = dbh.cursor()
    c.execute("""
        SELECT user_name FROM users ORDER BY user_name
    """)
    return list(c.fetchall())


def _find_active_instances_for(args, ec2, user_name):
    return ec2.get_all_instances(filters={
        'tag:saved_for_user': user_name,
        'instance-state-name': [
            'pending', 'running', 'stopping', 'stopped',
        ]
    })

def instances_by_user(args, ec2, user_set=None):
    result = {}
    instance_tags = ec2.get_all_tags({'resource_type': 'instance', 'key': 'saved_for_user'})
    for tag in instance_tags:
        user = tag.value
        if user_set and user not in user_set:
            continue
        instance = ec2.get_only_instances(instance_ids=[tag.res_id])[0]
        if instance.state == 'terminated' or instance.state == 'shutting-down':
            continue
        result.setdefault(user, []).append(instance)
    return result

def make_reservation_for(args, ec2, user_name, launch_args):
    launch_args['key_name'] = user_name
    launch_args['security_groups'] = launch_args.get('security_groups', []) + [args.default_security_group]
    reservation = ec2.run_instances(**launch_args)
    return list(reservation.instances)

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
                            instance.public_dns_name,
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
            time.sleep(POLL_DELAY)
    return failed_instances

def retag_instances(args, ec2, instances_by_user):
    for user, instances in instances_by_user.iteritems():
        for instance in instances:
            instance.add_tag('for_user', user)

def untag_instances(args, ec2, instances_by_user):
    for user, instances in instances_by_user.iteritems():
        for instance in instances:
            instance.remove_tag('for_user')

def wait_for_and_tag_instances(args, ec2, instances_by_user):
    pending_instances = instances_by_user.copy()
    for user in pending_instances:
        pending_instances[user] = list(pending_instances[user])
    failed_instances = {}
    start_time = time.time()
    while len(pending_instances) > 0 and time.time() - start_time < args.instance_pending_wait:
        for user in pending_instances.keys():
            instances = pending_instances[user]
            still_pending = []
            for instance in instances:
                status = instance.update()
                if status == 'pending':
                    still_pending.append(instance)
                elif status == 'running':
                    instance.add_tag('for_user', user)
                    instance.add_tag('saved_for_user', user)
                else:
                    failed_instances.setdefault(user, []).append(instance)
            if len(still_pending) > 0:
                pending_instances[user] = still_pending
            else:
                del pending_instances[user]
        time.sleep(POLL_DELAY)
    for user, instances in pending_instances.iteritems():
        failed_instances[user] = failed_instances.get(user, []) + instances
    return failed_instances


