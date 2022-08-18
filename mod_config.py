import configparser
import os


def getconfig(section, key):
    config = configparser.ConfigParser()
    path = os.path.split(os.path.abspath(__file__))[0] + '/mail.conf'
    print(path)
    config.read(path)
    return config.get(section, key)
