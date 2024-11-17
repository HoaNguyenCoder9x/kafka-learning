import configparser
config = configparser.ConfigParser()
config.sections()

config.read('sample.ini')

print(config['forge.example']['User'])

