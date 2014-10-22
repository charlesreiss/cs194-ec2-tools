#!/usr/bin/python
import argparse
import logging
import sys

import account_util

## startstop_instances.py --mode={start,stop} --ami=ami --type=type
def setup_args(parser):
    parser.add_argument('--mode', choices=['start','stop','terminate'], required=True)
    parser.add_argument('--ami', default='ami-b5a7ea85') # US West Oregon, HVM, 64-bit, Amazon Linux AMI
    parser.add_argument('--type', default='t2.micro')
    parser.add_argument('--users', default=None)

def start_instances(args):
    if args.users == None:
        dbh = account_util.connect_db()
        user_list = account_util.get_all_users()
        dbh.close()
    else:
        user_list = args.users.split(',')
    ec2 = account_util.connect_ec2(args)
    existing_instances = account_util.instances_by_user(args, ec2)
    started_instances = {}
    for user in user_list:
        if user not in existing_instances:
            started_instances[user] = account_util.make_reservation_for(args, ec2, user, {
                'image_id': args.ami,
                'instance_type': args.type,
            })
    failed_instances = account_util.wait_for_and_tag_instances(args, ec2, started_instances)
    started_instances.update(existing_instances)
    for k, v in account_util.healthcheck_instances(args, ec2, started_instances).iteritems():
        failed_instances[k] = failed_instances.get(k, []) + v
    for user, instances in failed_instances.iteritems():
        for instance in instances:
            try:
                instance.terminate()
            except boto.exception.EC2REsponseError, e:
                logging.error('error terminating instance %s: %s', instance.id, e)
    return failed_instances.keys()

def stop_instances(args):
    assert(not args.users) # not implemented
    ec2 = account_util.connect_ec2(args)
    for user, instances in account_util.instances_by_user(args, ec2).iteritems():
        for instance in instances:
            instance.stop()

def terminate_instances(args):
    assert(not args.users) # not implemented
    ec2 = account_util.connect_ec2(args)
    for user, instances in account_util.instances_by_user(args, ec2).iteritems():
        for instance in instances:
            instance.terminate()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    setup_args(parser)
    account_util.setup_args(parser)
    args = parser.parse_args()
    account_util.init_logging(args)

    if args.mode == 'start':
        if args.ami is None:
            logging.fatal('Need to specify AMI')
            sys.exit(1)
        failed = start_instances(args)
        if len(failed):
            logging.error('Failed to start instances for %s', sorted(failed))
    elif args.mode == 'stop':
        stop_instances(args)
    elif args.mode == 'terminate':
        terminate_instances(args)

