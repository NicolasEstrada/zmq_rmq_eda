import re
import smtplib

from email.parser import FeedParser


class SMTPServer(object):

    def __init__(self, smtp_server='localhost'):
        self.smtp_server_name = smtp_server
        self.smtp_server = smtplib.SMTP(self.smtp_server_name)

    def __del__(self):
        if self.smtp_server:
            self.smtp_server.quit()


class MailHandler(object):
    """MailHandler allows sends email"""

    def __init__(self, server):
        self.smtp = SMTPServer(server)

    def send_mail(self, sender, destinataries, subject, content):

        if not re.search('^.+@.+$', sender):
            raise ValueError('Sender address is not valid: {0}'.format(sender))

        parser = FeedParser()
        parser.feed('From: {0}\n'.format(sender))
        parser.feed('To: {1}\n'.format(', '.join(destinataries)))
        parser.feed('Subject: {2}\n'.format(subject))
        parser.feed('\n{3}\n'.format(content))

        mail = parser.close()

        self.smtp.sendmail(
            self.sender,
            destinataries,
            mail.as_string())
