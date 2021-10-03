# Import libraries
import os
import pandas as pd
import pandasql as ps
import logging

# Added for logging info and errors.
logging.basicConfig(level=logging.INFO)

# Local folder path name to be given as input to rpocess all the files
# file_path_input = r'C:\Users\bhanu\Downloads\Data Engineer Test\Data Engineer Test\Input_Data'
file_path_input = input("Enter the input file folder: ")


# Read csv file from the input folders.
def read_csv_file(file_path):
    """
    This function is to read the CSV file from the local folders and convert it to dataframe for further processing.
    :param file_path: The location for the input file path.
    :return: Returns the dataframe created out of the input CSV.
    """
    try:
        # Step to read the input csv file and convert the same to dataframe.
        with open(file_path, 'r') as f:
            csv_df = pd.read_csv(file_path)
        return csv_df

    except Exception as error:
        logging.error("Error while reading csv: {} ".format(str(error)))
        logging.error(logging.traceback.format_exc())


def requested_stats(final_df):
    """
    This function is to read the final dataframe and populated values like hottest day,temp on that day and it's corresponding region..
    :param final_df: The final dataframe to be considered to populated the hottest day,temp and region values.
    :return: NA
    """
    try:
        # Pandas sql to generate the stats from the final dataframe i.e. as mentioned in the question.
        hottest_temp = ps.sqldf(
            "select final_df.ObservationDate as Hottest_Date,final_df.Country as Hottest_Country,\
            final_df.Region as Hottest_Region,final_df.MaxScreenTemp as Max_Temp from final_df \
            join (select max(MaxScreenTemp) as hottest_temp from final_df) as test \
            on  final_df.MaxScreenTemp = test.hottest_temp")
        logging.info(hottest_temp)

    except Exception as error:
        logging.error("Error while performing stats: {} ".format(str(error)))
        logging.error(logging.traceback.format_exc())


if __name__ == '__main__':
    try:
        # The below step is to change the directory to the local folder before we start processing the input files.
        os.chdir(file_path_input)
        raw_data_final_df = pd.DataFrame()

        # This is to iterate through the directory to read all the files from that folder.
        for file in os.listdir():
            # Validate if the files are in csv format or not.
            if file.endswith(".csv"):
                file_path = f"{file_path_input}\{file}"

                # Post file validation read the file and convert the same into the dataframe.
                raw_data_df = read_csv_file(file_path)
                raw_data_final_df = raw_data_final_df.append(raw_data_df)

        if len(raw_data_final_df) > 1:
            # Listing the columns that we need for the final dataframe.Removed the unwanted columns from the input data.
            columns = ['ObservationTime', 'ObservationDate', 'ScreenTemperature', 'SignificantWeatherCode', \
                       'SiteName', 'Region', 'Country']

            # Creating a new dataframe to only have the required columns from the input data.
            df_selected_cols = raw_data_final_df[columns]

            # Creating the final dataframe which will be then used to populated the stats. This is one level up to the input data i.e. aggregated with respect to date by considering all the 24 observation points.
            # Assumption :-  Considering -99 as unknown value.
            final_df = ps.sqldf(
                "select Country,Region,SiteName,ObservationDate,max(ScreenTemperature) as MaxScreenTemp from df_selected_cols \
                where ScreenTemperature != -99 \
                Group by Country,Region,SiteName,ObservationDate")

            # Converting the final dataframe to parquet format.
            final_df.to_parquet('final_df.parquet.gzip', compression='gzip')

            # Calling the function to generate the stats mentioned in the question. This is an optional part as the final dataframe in the previous step should eb able to answer the questions.
            requested_stats(final_df)

        else:
            logging.error("Dataframe has no records except the headers")
            raise

    except Exception as error:
        logging.error("Error while performing operation: {} ".format(str(error)))
        logging.error(logging.traceback.format_exc())

    finally:
        logging.info("Execution Complete")
