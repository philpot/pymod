#!/usr/bin/python
# Filename: watlog.py

'''
watlog
@author: Andrew Philpot
@version 0.4

wat watlog module
Usage: python watlog.py
Options:
'''

REVISION = '$Revision: 21373 $',
import logging
import socket
 
#----------------------------------------------------------------------

# 'application' code
# logger.debug('debug message')
# logger.info('info message')
# logger.warn('warn message')
# logger.error('error message')
# logger.critical('critical message')

loggers = {}

def watlog(name):
    global loggers

    if loggers.get(name):
        return loggers.get(name)

    else:
        logger=logging.getLogger(name)
        logger.setLevel(logging.INFO)
        # logger.setLevel(logging.DEBUG)
        # create the logging file handler
        fh=logging.FileHandler('/nfs/studio-data/wat/log/wat.log')
        formatter = logging.Formatter('%(asctime)s [%(name)s] ::%(levelname)s:: %(message)s', datefmt='%Y%m%d %H:%M:%S')
        fh.setFormatter(formatter)
        # add handler to logger object
        logger.addHandler(fh)

        logger.info("------------------------------------------------------------------")
        logger.info("watlog consulted on %s" % (socket.gethostname()))

        loggers.update(dict(name=logger))
        return logger

# def main():
#     """
#     The main entry point of the application
#     """
#     if not already:
#         logger = logging.getLogger("wat")
#         logger.setLevel(logging.INFO)
#         # logger.setLevel(logging.DEBUG)
 
#         # create the logging file handler
#         fh = logging.FileHandler("/tmp/wat.log")
 
#         formatter = logging.Formatter('%(asctime)s [%(name)s] ::%(levelname)s:: %(message)s', datefmt='%Y%m%d %H:%M:%S')
#         fh.setFormatter(formatter)
 
#         # add handler to logger object
#         logger.addHandler(fh)
 
#         logger.info("------------------------------------------------------------------")
#         logger.info("watlog consulted on %s" % (socket.gethostname()))
        
# if __name__ == "__main__":
#     main()
