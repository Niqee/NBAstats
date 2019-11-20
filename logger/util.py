from datetime import datetime


# TODO: Add clean logs methods
class Logger(object):
    def __init__(self, logs_dir_path: str = "logger/logs"):
        self.log_path_blank = logs_dir_path + r"/{}"

        self.log_success_path = self.log_path_blank.format("log_ok.txt")
        self.log_error_path = self.log_path_blank.format("log_err.txt")

    def log_error(self, url, error_msg):
        current_time = datetime.now()
        with open(self.log_error_path, "a+") as log_err_file:
            log_err_file.write("{time}|{timestamp}|{url}|{msg}\n".format(time=current_time,
                                                                         timestamp=current_time.timestamp(),
                                                                         url=url,
                                                                         msg=error_msg))

    def log_success(self, url):
        current_time = datetime.now()
        with open(self.log_success_path, "a+") as log_ok_file:
            log_ok_file.write("{time}|{timestamp}|{url}\n".format(time=current_time,
                                                                  timestamp=current_time.timestamp(),
                                                                  url=url))
