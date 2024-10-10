import pandas as pd
import re
import os
import emoji
from logger import Logger

class DataCleaner:
    # Constants for column names
    CHANNEL_USERNAME = 'Channel Username'
    MESSAGE = 'Message'
    DATE = 'Date'
    ID = 'ID'
    
    def __init__(self):
        """
        Initialize the DataCleaner with a custom logger instance.
        """
        self.logger = Logger(log_file='../data/cleaner_log.log')
        self.allowed_characters = re.compile(r'[^a-zA-Z0-9\s.,!?;:()[]@&]+')

    def load_data(self, file_path):
        """Loads data from a CSV file."""
        try:
            df = pd.read_csv(file_path)
            self.logger.info(f"Data loaded successfully. Shape: {df.shape}")
            return df
        except FileNotFoundError:
            self.logger.error("File not found. Please check the file path.")
            return pd.DataFrame()  # Return empty DataFrame for consistency
        except Exception as e:
            self.logger.error(f"An error occurred while loading data: {str(e)}")
            return pd.DataFrame()

    def remove_duplicates(self, df, image_directory):
        """Removes duplicate entries and corresponding images."""
        duplicates = df[df.duplicated(subset=self.ID, keep='first')]
        df = df.drop_duplicates(subset=self.ID, keep='first')

        self.logger.info(f"Duplicates removed. New shape: {df.shape}")
        self._remove_duplicate_images(duplicates, image_directory)

        return df

    def _remove_duplicate_images(self, duplicates, image_directory):
        """Removes images corresponding to duplicate entries."""
        for index, row in duplicates.iterrows():
            channel_username = row[self.CHANNEL_USERNAME]
            message_id = row[self.ID]
            image_name = f"{channel_username}_{message_id}.jpg"
            image_path = os.path.join(image_directory, image_name)

            if os.path.exists(image_path):
                try:
                    os.remove(image_path)
                    self.logger.info(f"Removed duplicate image: {image_path}")
                except Exception as e:
                    self.logger.error(f"Error removing image: {image_path}. Exception: {str(e)}")

    def handle_missing_values(self, df):
        """Handles missing values in the dataset."""
        df.fillna({
            self.CHANNEL_USERNAME: 'Unknown',
            self.MESSAGE: 'N/A',
            self.DATE: '1970-01-01 00:00:00'
        }, inplace=True)
        self.logger.info("Missing values handled.")
        return df

    def standardize_formats(self, df):
        """Standardizes the formats of columns like Date, message content, and channel name."""
        # Convert Date column
        if self.DATE in df.columns:
            df[self.DATE] = pd.to_datetime(df[self.DATE], errors='coerce')

        # Clean and format message content
        if self.MESSAGE in df.columns:
            df[self.MESSAGE] = df[self.MESSAGE].apply(self.clean_message_content).str.lower().str.strip()

        # Clean and format channel names
        if self.CHANNEL_USERNAME in df.columns:
            df[self.CHANNEL_USERNAME] = df[self.CHANNEL_USERNAME].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True).str.strip().str.title()

        self.logger.info("Formats standardized.")
        return df

    def clean_message_content(self, text):
        """Cleans the message content by removing unwanted characters."""
        # Remove emojis
        text = emoji.replace_emoji(text, replace='')  # Remove emojis
        # Remove unwanted characters but keep specific patterns intact
        text = re.sub(self.allowed_characters, '', text)  # Remove unwanted characters
        #text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces and newlines

        return text

    def validate_data(self, df):
        """Validates the data for any inconsistencies."""
        # Drop rows with invalid Dates
        df = df.dropna(subset=[self.DATE])

        # Validate message content length
        if self.MESSAGE in df.columns:
            df = df[df[self.MESSAGE].str.len() <= 1000]

        # Validate channel names
        df = df[df[self.CHANNEL_USERNAME].str.len() > 0]

        self.logger.info("Data validation completed.")
        return df

    def save_cleaned_data(self, df, file_path):
        """Saves the cleaned data to a CSV file."""
        cleaned_file_path = file_path.replace('.csv', '_cleaned.csv')
        df.to_csv(cleaned_file_path, index=False)
        self.logger.info(f"Cleaned data saved to {cleaned_file_path}.")

    def clean_telegram_data(self, file_path, image_directory):
        """
        Main function to clean Telegram data stored in a CSV file and remove corresponding duplicate images.
        
        Parameters:
        file_path (str): The path to the CSV file containing Telegram data.
        image_directory (str): The directory path where images are stored.
        
        Returns:
        pd.DataFrame: A cleaned pandas DataFrame.
        """
        try:
            # Load the data
            df = self.load_data(file_path)
            if df.empty:
                self.logger.error("No data loaded, cleaning process aborted.")
                return df
            
            # Run cleaning steps
            df = self.remove_duplicates(df, image_directory)
            df = self.handle_missing_values(df)
            df = self.standardize_formats(df)
            df = self.validate_data(df)
            
            # Save cleaned data
            self.save_cleaned_data(df, file_path)
            
            self.logger.info("Data cleaning completed successfully.")
            return df

        except Exception as e:
            self.logger.error(f"An error occurred during the cleaning process: {str(e)}")
            return pd.DataFrame()

# Run the function
if __name__ == '__main__':
    # Class instance
    cleaner = DataCleaner()
    # Call the main telegram cleaner function
    cleaner.clean_telegram_data('../data/telegram_data.csv', '../data/photos/')
