from config import Configuration


class GenericDataCollector(object):
    """
    Generic data collection class that will have methods for both interfacing with a data source (API, FTP, etc.) to
    gather data, as well as methods for cleaning / processing data in preparation for saving.
    """

    def __init__(self):
        """
        Load in configuration settings.
        """
        self.settings = Configuration().settings  # Any API Keys / Login info should be in this dictionary

    def get_raw_data(self):
        """
        Method for gathering data, this is very application specific.
        :return: raw_data
        """
        data = None
        # Write collection code here
        return data

    def get_processed_data(self):
        """
        Method for cleaning/processing the raw_data
        :return: cleaned data ready to be saved.
        """
        raw_data = self.get_raw_data()
        processed_data = None
        # processed_data = [do_something(row) for row in raw_data]
        return processed_data
