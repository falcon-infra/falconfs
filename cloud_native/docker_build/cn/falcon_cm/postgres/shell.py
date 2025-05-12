import logging
import subprocess
import sys
import os

def exec_cmd(cmd):
    logger = logging.getLogger('logger')
    logger.info(f'Execute command : {cmd}')
    result = subprocess.getoutput(cmd)
    logger.info(f'Command result : {result}')
    return result

def get_current_dir():
    if getattr(sys, 'frozen', False):
        app_path = os.path.dirname(sys.executable)
    else:
        app_path = os.path.dirname(__file__) + '/../'
    return app_path
    