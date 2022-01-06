from typing import Optional
import pandas as pd 
from pathlib import Path 
from app import utils 
from sys import argv, exit
logger = utils.logger(__name__)

#set argv for debug



#get new files
def get_new_files(path : str) -> list:

    if not Path(path).is_dir():
        raise Exception('Path is not a directory')
        logger.error('Path is not a directory')
    


    files = list(Path(path).glob('*.csv'))
    logger.info(f'{len(files)} files found')

    new_files = check_for_processed_files(files)
    logger.info(f'{len(new_files)} files found to process')



    return new_files

def check_for_processed_files(files : list) -> list:
    
    trg_file = Path(__file__).parent.joinpath('process_log')
    if not Path(trg_file).is_dir():
        logger.info('No processed files found first run')
        return files
    else:
        log_files = pd.read_parquet(trg_file,engine='fastparquet')
        file_df =  pd.DataFrame([(f.stem, f)for f in files], columns=['file_name','file_path'])
        return file_df.loc[~file_df['file_name'].isin(log_files['file_name'])]['file_path'].tolist()

#get relevant columns from file
def get_relevant_columns(file : Path,
                        target_cols : Optional[list] = ['new to nc fusion','player_id','ethnicity']) -> None:

    df = pd.read_csv(file)
    logger.debug(f'the following columns are present in the file: {df.columns.tolist()}')

    df = df.loc[:,df.columns.str.contains('|'.join(target_cols),case=False)]
    logger.debug(f'the following columns were found: {df.columns.tolist()} in {file.name}')

    file_dt = create_iso_date(file)
    
    move_file(file_dt, Path(file_dt).parent.joinpath('processed'))
    
    curated_file = Path(file_dt).parent.joinpath('curated',f"{file_dt.stem}.csv")

    df.columns = df.columns.str.lower().str.strip()
    df.columns = df.columns.str.replace('(ethnicity).*',r'\1',regex=True)

    df.to_csv(curated_file, index=False)
    logger.debug(f'file was moved to curated folder - {curated_file}')

    return df.assign(file_name=file.stem)

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


    
def log_file_metadata(data_frames : list) -> None:

    final = pd.concat(data_frames)
    final['program_code'] = final['file_name'].str.split('-',expand=True)[1].fillna(-1).astype(int)
    final['extract_year'] = final['file_name'].str.extract('-(\d{4})_').fillna(-1).astype(int)
    final['ingest_year'] = pd.Timestamp('now').year

    df2 = (final.loc[final['program_code'].ne(-1)]
        .melt(id_vars=['file_name','program_code','extract_year'],var_name='column_name')
        .drop_duplicates(subset=['program_code','column_name'])
        .dropna(subset=['value'])
        .groupby(['file_name','program_code','extract_year','column_name'])['column_name'].count().unstack(-1)
        .reset_index()
        
        ).fillna(0)

    trg_path = Path(__file__).parent.joinpath('process_log')
    df2.to_parquet(trg_path,partition_cols=['extract_year'],engine='fastparquet')



if __name__ == '__main__':
    #catch any exception 
    try:
        logger.info('starting')
        # argv = ['.\\main.py',r"C:\Users\umarh\Documents\expertmoney\test"]
        logger.info(f'argv: {argv[0], argv[1]}')

        file_path = Path(argv[1])

        if not file_path.is_dir():
            raise Exception(f'Path : {file_path} is not a directory')
            logger.error('Path is not a directory')

        files = get_new_files(file_path)
        dfs = []
        for file in files:
            dfs.append(get_relevant_columns(file))

        if len(dfs) > 0:
            log_file_metadata(dfs)
        else:
            logger.info('no files to process')
        logger.info('finished')
    except Exception as e:
        logger.error(e)
        logger.error('exiting')
        exit(1)
    
