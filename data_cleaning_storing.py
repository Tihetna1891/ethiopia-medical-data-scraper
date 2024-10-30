import os
import pandas as pd
from PIL import Image
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Database configuration
db_config = {
    'host': "localhost",
    'port': "5432",
    'user': "postgres",
    'password': "1891",
    'dbname': "telegram_data"  # Your PostgreSQL database name
}

# Create database connection using SQLAlchemy
engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

# Define paths
text_folder_path = 'C:/Users/dell/ethiopia-medical-data-scraper/telegram_data/'
image_folder_path = 'C:/Users/dell/ethiopia-medical-data-scraper/telegram_data/images/'

# List of text files for each Telegram channel
text_file_names = ['DoctorsET.txt', 'EAHCI.txt', 'lobelia4cosmetics.txt', 'yetenaweg.txt']
text_data_frames = []

# Processing text files
for file_name in text_file_names:
    file_path = os.path.join(text_folder_path, file_name)
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.readlines()
        
        # Convert to DataFrame
        df = pd.DataFrame(data, columns=['text'])
        df['channel'] = file_name  # Add channel name column
        text_data_frames.append(df)
    except Exception as e:
        logging.error(f"Failed to read {file_name}: {e}")

# Combine all text DataFrames into one
df_text_combined = pd.concat(text_data_frames, ignore_index=True)

# Clean text data
df_text_combined = df_text_combined.drop_duplicates(subset=['text'])
df_text_combined = df_text_combined.dropna(subset=['text'])
df_text_combined.reset_index(drop=True, inplace=True)
df_text_combined['message_id'] = df_text_combined.index + 1

# Ensure data types are compatible
df_text_combined['channel'] = df_text_combined['channel'].astype(str)
df_text_combined['text'] = df_text_combined['text'].astype(str)

# Image data processing
image_data = []
for image_name in os.listdir(image_folder_path):
    image_path = os.path.join(image_folder_path, image_name)
    try:
        # Validate if the image is readable by opening it
        with Image.open(image_path) as img:
            img.verify()  # This will raise an exception if the image is corrupt
        
        # Store image metadata
        image_data.append({'image_name': image_name, 'image_path': image_path})
    except Exception as e:
        logging.error(f"Invalid image {image_name}: {e}")

# Convert image data to DataFrame
df_image = pd.DataFrame(image_data)

# Ensure image DataFrame types are compatible
df_image['image_name'] = df_image['image_name'].astype(str)
df_image['image_path'] = df_image['image_path'].astype(str)

# Save data to PostgreSQL database
try:
    # Save text data
    df_text_combined.to_sql('telegram_messages', engine, if_exists='append', index=False)
    logging.info("Text data inserted successfully.")
except Exception as e:
    logging.error(f"Error inserting text data: {e}")

try:
    # Save image metadata
    df_image.to_sql('telegram_images', engine, if_exists='append', index=False)
    logging.info("Image metadata inserted successfully.")
except Exception as e:
    logging.error(f"Error inserting image metadata: {e}")

print("Text and image data cleaning and storage completed.")
