from config import FileLogger, LoggerWriter, EmailConfig
from database import SQLCaller
from data_collection import GenericDataCollector
import sys

email = EmailConfig()
file_logger = FileLogger().get_logger()
sys.stderr = LoggerWriter(file_logger)
sys.stdout = LoggerWriter(file_logger)

data_collector = GenericDataCollector()
sql = SQLCaller()

try:
    save_data = data_collector.get_processed_data()
    sql.save_new_data(save_data, 'table_name')
    email.email_message('Message')
except Exception as e:
    file_logger.exception(e)
