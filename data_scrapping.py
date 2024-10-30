import os
import logging
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from telethon.errors import FloodWaitError

# Set up logging
logging.basicConfig(filename='scraping_log.log', level=logging.INFO)

# Telegram API credentials
api_id = '29046206'
api_hash = 'a63749caf97d47c84397ed5640ce7b8e'
client = TelegramClient('session_name', api_id, api_hash)

# Channels to scrape
channels = [
    'https://t.me/DoctorsET',
    'https://t.me/lobelia4cosmetics',
    'https://t.me/yetenaweg',
    'https://t.me/EAHCI',
    'https://t.me/Chemed'
]

# Storage directories
data_dir = 'telegram_data'
images_dir = os.path.join(data_dir, 'images')

os.makedirs(data_dir, exist_ok=True)
os.makedirs(images_dir, exist_ok=True)

def get_channel_name(url):
    """Extracts the channel name from a full Telegram URL."""
    return url.split('/')[-1]

async def fetch_messages(channel):
    channel_name = get_channel_name(channel)  # Cleaned channel name for the file
    text_file_path = os.path.join(data_dir, f'{channel_name}.txt')
    
    async for message in client.iter_messages(channel):
        # Save text content
        if message.text:
            with open(text_file_path, 'a', encoding='utf-8') as f:
                f.write(f'{message.date}: {message.text}\n')
        
        # Download image if present
        if message.media and isinstance(message.media, MessageMediaPhoto):
            try:
                file_path = await client.download_media(message, images_dir)
                logging.info(f'Downloaded image to {file_path}')
            except Exception as e:
                logging.error(f'Failed to download media: {e}')

async def main():
    await client.start()
    logging.info('Connected to Telegram')
    for channel in channels:
        try:
            await fetch_messages(channel)
            logging.info(f'Successfully scraped {channel}')
        except FloodWaitError as e:
            logging.warning(f'FloodWaitError: Sleeping for {e.seconds} seconds')
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logging.error(f'Failed to scrape {channel}: {e}')

with client:
    client.loop.run_until_complete(main())