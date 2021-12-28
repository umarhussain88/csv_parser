from typing import Optional
import pandas as pd 
from pathlib import Path 
from app import utils 
from sys import argv

logger = utils.logger(__name__)




#get new files
def get_new_files(path : str) -> list:

    if not Path(path).is_dir():
        raise Exception('Path is not a directory')
        logger.error('Path is not a directory')
    


    files = list(Path(path).glob('*.csv'))
    logger.info(f'{len(files)} files found')

    return files

#get relevant columns from file
def get_relevant_columns(file : Path,
                        target_cols : Optional[list] = ['new to nc fusion','player_id','ethnicity']) -> None:

    df = pd.read_csv(file)
    logger.info(f'the following columns are present in the file: {df.columns.tolist()}')

    df = df.loc[:,df.columns.str.contains('|'.join(target_cols),case=False)]
    logger.info(f'the following columns were found: {df.columns.tolist()} in {file.name}')

    file_dt = create_iso_date(file)
    move_file(file_dt, file.parent.joinpath('processed'))
    
    curated_file = file_dt.parent.joinpath('curated',f"{file_dt.stem}.csv")

    df.columns = df.columns.str.lower().str.strip()
    df.columns = df.columns.str.replace('(ethnicity).*',r'\1',regex=True)

    df.to_csv(curated_file, index=False)
    logger.info(f'file was moved to curated folder - {curated_file}')

    return df.assign(file_name=file.stem)

#create iso date for file
def create_iso_date(file : Path) -> str:
    
    dt = pd.Timestamp('now').strftime('%Y_%m_%d_%H_%M_%S')

    return file.rename(file.parent.joinpath(f'{dt}_{file.name}'))


#move file to new location
def move_file(file : Path, processed_path : Path) -> None:
    
        logger.info(f'moving {file.name} to {processed_path}')
        #create location if not exists
        if not Path(processed_path).is_dir():
            Path(processed_path).mkdir(parents=True)
            logger.info(f'created {processed_path}')
        #creat curataed location
        if not Path(file.parent.joinpath('curated')).is_dir():
            Path(file.parent.joinpath('curated')).mkdir(parents=True)
            logger.info(f'created {file.parent.joinpath("curated")}')
        
        file.rename(file.parent.joinpath(processed_path).joinpath(file.name))
        logger.info(f'moved {file.name} to {processed_path}')


    
def log_file_metadata(data_frames : list) -> None:

    final = pd.concat(data_frames)

    df2 = (final.loc[final['program_code'].ne(-1)]
        .melt(id_vars=['file_name','program_code','extract_year'],var_name='column_name')
        .drop_duplicates(subset=['program_code','column_name'])
        .dropna(subset=['value'])
        .groupby(['file_name','program_code','extract_year','column_name'])['column_name'].count().unstack(-1)
        .reset_index()
        
        ).fillna(0)
    
    trg_path = Path(__file__).joinpath('files/process_log')
    df2.to_parquet(trg_path,partition_cols=['extract_year'],engine='fastparquet')






if __name__ == '__main__':
    logger.info('starting')
    logger.info(f'argv: {argv[1]}')

    file_path = Path(argv[1])

    if not file_path.is_dir():
        raise Exception(f'Path : {file_path} is not a directory')
        logger.error('Path is not a directory')

    files = get_new_files(file_path)
    dfs = []
    for file in files:
        dfs.append(get_relevant_columns(file))

    log_file_metadata(dfs)
    

    logger.info('finished')
    
