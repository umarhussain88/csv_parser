import logging
from pathlib import Path



#create logger
def logger(name : str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    #create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    if not Path(__file__).parent.parent.joinpath('logs').exists():
        Path(__file__).parent.parent.joinpath('logs').mkdir(parents=True)  
    #create file handler and set level to warning
    fh = logging.FileHandler('logs/app.log', mode='a')
    fh.setLevel(logging.INFO)
    
    #create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    #add formatter to ch
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    
    #add ch to logger
    logger.addHandler(ch)
    #add fh to logger
    logger.addHandler(fh)
    
    
    return logger



