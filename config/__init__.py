from dotenv import load_dotenv
import os
import logging
from datetime import datetime
import logging.handlers
import pathlib
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Configuration(object):
    """
    Object for interfacing with sensitive data via .env file
    """

    def __init__(self):
        """
        Load in and store configuration settings from .env
        """
        cur_dir = pathlib.Path(__file__).parent.parent.absolute()
        load_dotenv(cur_dir / 'config' / '.env')
        self.settings = {
            'SQL_URI': os.environ.get('SQL_URI'),
            'TO_ADDRESS': os.environ.get('DEBUG_ADDRESS'),
            'CC_ADDRESS': os.environ.get('CC_ADDRESS'),
            # Include any other sensitive information required here such as API Keys or FTP credentials
        }


class FileLogger(object):
    def __init__(self, log_level: str = 'DEBUG'):
        """
        Logging Class to automatically create and write log files according to the entered log-level.
        Defaults to the top-level logs directory of project. Log files will have following name conventions
        log_mmddYYYY.txt where the date is the date on which the pipeline was run.
        :param str log_level: The desired log level
        """
        now = datetime.now().strftime('%m%d%y')
        log_base_path = pathlib.Path(__file__).parent.parent.absolute() / 'logs'
        self.LOG_LEVEL = log_level
        log_file = str(log_base_path / f'log_{now}.txt')
        self.__logger = logging.getLogger(__name__)
        self.__logger.setLevel(self.LOG_LEVEL)
        handler = logging.FileHandler(filename=log_file)
        self.__handler = handler
        self.__logger.addHandler(handler)
        self.__logger.info('Logging Initialized!')

    def get_logger(self, level: str = None) -> logging.Logger:
        """
        Method to obtain the FileLogging objects underlying logging.Logger object
        :param str level: (Optional) The new log level if one is required since initialization
        :return: The file logging object
        """
        if level:
            self.__logger.setLevel(level)
        return self.__logger

    def get_handler(self) -> logging.FileHandler:
        """
        Method to obtain the FileLogging objects underlying logging.FileHandler object.
        :return: The file handler
        """
        return self.__handler


class LoggerWriter(object):
    def __init__(self, logger):
        """
        This class will allows us to write system outputs using a supplied logging object.
        :param logging.Logger logger: A Logger object with which to write.
        """
        self.logger = logger

    def write(self, message: str):
        """
        Method for manually writing messages to the logs via our supplied logger.
        If stdout and stderr are being captured, this can be circumvented using simple
        print statements.
        :param str message: Message to write to logs.
        :return: Nothing
        """
        if message != '\n':
            self.logger.info(message)

    def flush(self):
        """
        Required method for logger to capture stdout / stderr info
        :return: Nothing
        """
        pass


class EmailConfig(object):
    def __init__(self):
        configs = Configuration()
        self.to_adds = configs.settings['TO_ADDRESS']
        self.cc = configs.settings['CC_ADDRESS']
        self.message = ''

    def email_message(self, message: str) -> None:
        """
        Email message report, can use to report status of pipeline / errors.
        :param message: Message to append to the end of the report summary.
        :return: Nothing
        """
        from_address = 'foo@email.com'  # Email Address for the pipeline.
        string = 'This is an auto-generated email\n.'
        string += message
        message_text = string
        rcpt = self.cc.split(',') + [self.to_adds]
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Subject"  # Subject for Reporting Emails
        msg['To'] = self.to_adds
        msg['Cc'] = self.cc
        msg.attach(MIMEText(message_text))
        self.message = message_text
        server = smtplib.SMTP('smtp.server')  # SMTP Server to Relay Message
        server.sendmail(from_address, rcpt, msg.as_string())
        server.quit()
