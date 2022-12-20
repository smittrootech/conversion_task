import argparse
from os.path import exists
from os import listdir
import pandas as pd
import os



'''
Function to check directories exist or not
'''
def check_if_directory_exist(input_path:str, output_path:str,suffix=".csv" ):
    if exists(input_path) and exists(output_path):
            filenames = listdir(input_path)
            return [ filename for filename in filenames if filename.endswith( suffix ) ]
    else:
        raise FileNotFoundError("File not found")


'''
Reading files
'''
def read_files(input_files:str) -> pd.DataFrame:
    pandas_dataframe=pd.read_csv(input_files)
    return pandas_dataframe


'''
Writing Files
'''
def write_files(input_data:pd.DataFrame,output_path):
    input_data.to_csv(output_path)
    print("Writing data is completed")
    


def data_manipulation(dataframe:pd.DataFrame,prefix_filename)-> pd.DataFrame:

    '''
    Replacing unwanted punctuations to get desired column names as an output 
    '''
    dataframe.iloc[0]=dataframe.columns
    for index,col in enumerate(dataframe.iloc[0]):
        replaced_value=dataframe.iloc[0][index].replace("(","").replace(")","").replace(" #","-").replace(" ","-").replace("|","_").replace(".","")
        dataframe.loc[0, col]=f"[{prefix_filename}-{replaced_value}-kWh]"

    '''
    Removing Unnamed columns if exists while reading the dataframe
    '''

    dataframe = dataframe.loc[:, ~dataframe.columns.str.contains('^Unnamed')]

    '''
    Creating Datetime as index for further usage
    '''
    dataframe.iloc[0]['Date and time']='Datetime'
    dataframe.columns=dataframe.iloc[0]
    dataframe=dataframe[1:].set_index('Datetime')

    
    '''
    Converting index to datetime so can be used for interval creation 
    '''  
    dataframe.index = pd.to_datetime(dataframe.index)

    '''
    Converting datatypes into floating datatype. Hence can be used for future summation
    '''
    all_columns=list(dataframe.columns)
    for data_type_change in all_columns:
        dataframe[data_type_change]=dataframe[data_type_change].astype('float')
    return dataframe


def resampling_and_conversion(interval_time,dataframe:pd.DataFrame):

    '''
    Creating Interval of n mins which we can vary based on requirement and filling null with blank
    Also adding data till end of the day if not present
    '''

    dataframe = dataframe.fillna('')
    dataframe=dataframe.resample(f'{interval_time}T').sum()
    df1=pd.date_range(start=str(dataframe.index[0]), freq= f'{interval_time}T', end=f'{str(dataframe.index[-1].date())} 23:59:00')
    dataframe=dataframe.reindex(df1)
    dataframe.index.names=["Datetime"]

    '''
    Applying division of 1000 to convert Wh to KWh and dataconversion to avoid mathematical error for divison to Kwh
    This -1 could be changed to any value if required.
    '''
    dataframe=dataframe.replace("",-1)
    dataframe=dataframe.apply(lambda x: x/1000 )
    dataframe=dataframe.replace(-0.001,'')

    '''
    Changing datetime to desired ouput as requested in task
    '''

    file_date=str(dataframe.index[0]).split(" ")[0].replace("-","")
    dataframe.index=dataframe.index.strftime('%d/%m/%Y %-H:%M')
    return dataframe,file_date


if __name__=="__main__":

    help = 'Conversion Script'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input' ,required=True,help='input folder of file')
    parser.add_argument('-o','--output',required=True, help='output folder of file')
    parser.add_argument('-inter','--interval', required=True, help='interval minutes')

    args = parser.parse_args()

    files=check_if_directory_exist(args.input,args.output)
    for file in files:
        pandas_dataframe=read_files(os.path.join(args.input,file))
        prefix_filename=file.split("_")[0]
        manipulated_data=data_manipulation(pandas_dataframe,prefix_filename)
        output_data,file_date=resampling_and_conversion(args.interval,manipulated_data)
        output_filename="Electricity_"+ file_date + "_130000_V1.csv"
        write_files(output_data,os.path.join(args.output,output_filename))

