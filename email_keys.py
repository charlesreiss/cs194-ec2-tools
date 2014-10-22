#!/usr/bin/python
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase

import argparse
import codecs
import logging
import socket
import os.path
import ssl
from smtplib import SSLFakeFile

import account_util

LOGGER = logging.getLogger(__name__)

def setup_args(parser):
    group = parser.add_argument_group('email_keys')
    group.add_argument('--template_file', default='email_template.txt')
    group.add_argument('--subject', default='[CS194-16] EC2 credentials for HW4')
    group.add_argument('--from', dest='from_', default='charles.reiss+cs194@berkeley.edu')
    group.add_argument('--cc', default=None)

    group.add_argument('--smtp_gateway')
    group.add_argument('--smtp_username')
    group.add_argument('--smtp_password')

    group.add_argument('--smtp_certs', default='/etc/ssl/certs/AddTrust_External_Root.pem')

    group.add_argument('--dry_run', action='store_true', default=False)

class MySMTP_SSL(smtplib.SMTP):
    default_port = 465

    def __init__(self, host='', port=0, local_hostname=None,
                 ca_certs=None,
                 timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        self.ca_certs = ca_certs
        smtplib.SMTP.__init__(self, host, port, local_hostname, timeout)

    def _get_socket(self, host, port, timeout):
        if self.debuglevel > 0:
            print>>stderr, 'connect:', (host, port)
        new_socket = socket.create_connection((host, port), timeout)
        new_socket = ssl.wrap_socket(new_socket, cert_reqs=ssl.CERT_REQUIRED,
                                      ca_certs=self.ca_certs)
        self.file = SSLFakeFile(new_socket)
        return new_socket

def generate_email(args, group_name, to_emails, account, password, key_file):
    message = MIMEMultipart()
    message['Subject'] = args.subject
    message['From'] = args.from_
    message['To'] = ', '.join(to_emails)
    if args.cc:
        message['Cc'] = args.cc
    with codecs.open(args.template_file, 'r', 'utf-8') as fh:
        template = fh.read() 
        filled_template = template.format(
            group_name=group_name,
            account=account,
            password=password,
        )
    message.attach(MIMEText(filled_template.encode('utf-8'), 'plain', 'UTF-8'))

    with open(key_file, 'r') as fh:
        sub_message = MIMEBase('application', 'octet-stream')
        sub_message.set_payload(fh.read())
        sub_message.add_header('Content-Disposition', 'attachment',
            filename=os.path.basename(key_file) + '.pem')
    message.attach(sub_message)

    return message

def send_email(args, message, to_emails):
    server = MySMTP_SSL(host=args.smtp_gateway, ca_certs=args.smtp_certs)
    server.login(args.smtp_username, args.smtp_password)
    all_to_emails = to_emails
    if args.cc:
        all_to_emails = all_to_emails + [args.cc]
    server.sendmail(args.from_, all_to_emails, message.as_string())
    server.quit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    account_util.setup_args(parser)
    setup_args(parser)
    args = parser.parse_args()

    LOGGER.setLevel(logging.DEBUG)

    account_util.init_logging(args)
    
    dbh = account_util.connect_db(args)
    passwords = account_util.get_all_passwords(args, dbh)
    dbh.close()
    with codecs.open(args.users_from_list, 'r', 'utf-8') as fh:=
        for line in fh:
            parts = line.strip().split('\t')
            group_name = parts[0]
            group_account = parts[1]
            to_emails = parts[2:]
            password = passwords[group_account]
            message = generate_email(args, group_name, to_emails, group_account, password,
                os.path.join(args.ssh_key_dir, group_account))
            LOGGER.debug('message is %s', message)
            if not args.dry_run:
                send_email(args, message, to_emails)
