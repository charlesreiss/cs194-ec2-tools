#!/usr/bin/python
import account_util
import logging

## startstop_instances.py --mode={start,stop} --ami=ami --type=type
def setup_args(parser):
    parser.add_argument('--mode', choices=['start','stop'])
    parser.add_argument('--ami', default='')
    parser.add_argument('--type', default='t1.tiny')

def start_instances(args, user_list):
    ec2 = boto.connect_ec2()
    existing_instances = instances_by_user(args, ec2)
    for user in user_list:
        if user not in existing_instances:
            started_instances[user] = make_reservation_for(args, ec2, user, {
                'image': args.ami,
                'instance_type': args.type
            })
    failed_instances = wait_for_and_tag_instances(args, ec2, started_instances)
    started_instances.update(existing_instances)
    for k, v in healthcheck_instances(args, ec2, started_instances)).iteritems():
        failed_instances.setdefault(k, []) += v
    for user, instances in failed_instances.iteritems():
        for instance in instances:
            try:
                instance.terminate()
            except boto.exception.EC2REsponseError, e:
                logging.error('error terminating instance %s: %s', instance.id, e)
