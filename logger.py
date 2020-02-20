import sys
import logging
from logging.handlers import TimedRotatingFileHandler
import os

def writeLog(message):
    logging.basicConfig(level=logging.WARNING,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        filemode='a')
    formatter = logging.Formatter('%(asctime)s:%(filename)s:%(funcName)s:[line:%(lineno)d] %(levelname)s %(message)s')
    CURRENT_DIR = os.path.dirname(__file__)
    LOG_FILE = os.path.abspath(os.path.join(CURRENT_DIR, "logs", "runtime.log"))
    fileTimeHandler = TimedRotatingFileHandler(LOG_FILE, "D", 1, 0,encoding='utf-8')
    fileTimeHandler.suffix = "%Y%m%d.log"
    fileTimeHandler.setFormatter(formatter)
    loggers = logging.getLogger('')
    loggers.addHandler(fileTimeHandler)
    loggers.warn(message)
    loggers.handlers.pop()