from typing import Optional
import pandas as pd 
from pathlib import Path 
from app import utils 


logger = utils.logger(__name__)



#get new files
def get_new_files(path : str) -> list:

    if not Path(path).is_dir():
        raise Exception('Path is not a directory')
        logger.error('Path is not a directory')
    


    files = list(Path(path).glob('*.xlsx*'))
    logger.debug(f'{len(files)} files found')

    return files

#get relevant columns from file
def get_relevant_columns(file : Path,
                        target_cols : Optional[list] = ['new to nc fusion','player_id','ethnicity']) -> None:

    df = pd.read_excel(file, sheet_name='Sheet2', engine='openpyxl')
    logger.debug(f'the following columns are present in the file: {df.columns.tolist()}')

    df = df.loc[:,df.columns.str.contains('|'.join(target_cols),case=False)]
    logger.debug(f'the following columns were found: {df.columns} in {file.name}')

    file_dt = create_iso_date(file)
    move_file(file_dt, file.parent.joinpath('processed'))
    
    df.to_csv(file_dt.parent.joinpath('curated',f"{file_dt.stem}.csv"), index=False)


#create iso date for file
def create_iso_date(file : Path) -> str:
    
    dt = pd.Timestamp('now').strftime('%Y_%m_%d_%H_%M_%S')

    return file.rename(file.parent.joinpath(f'{dt}_{file.name}'))


#move file to new location
def move_file(file : Path, processed_path : Path) -> None:
    
        logger.debug(f'moving {file.name} to {processed_path}')
        #create location if not exists
        if not Path(processed_path).is_dir():
            Path(processed_path).mkdir(parents=True)
            logger.debug(f'created {processed_path}')
        #creat curataed location
        if not Path(file.parent.joinpath('curated')).is_dir():
            Path(file.parent.joinpath('curated')).mkdir(parents=True)
            logger.debug(f'created {file.parent.joinpath("curated")}')
        
        file.rename(file.parent.joinpath(processed_path).joinpath(file.name))
        logger.debug(f'moved {file.name} to {processed_path}')


    
         



if __name__ == '__main__':
    logger.info('starting')
    files = get_new_files('/home/umarh/csv_parser/files')
    for file in files:
        get_relevant_columns(file)
    logger.info('finished')
    