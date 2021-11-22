from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import GetMessagesRequest
from telethon.tl.types import PeerChat
import gspread
import  time
import config

url = config.spreadsheet_url
gc = gspread.service_account(filename=config.spread_user_cred_json_file_addr)
gsheet = gc.open_by_url(url)
worksheet = gsheet.worksheets()[0]


client = TelegramClient(config.name,config.api_id,config.api_hash)


def insert_spread(values=[]):
    if len(values)!=0:
        worksheet.append_row(values)
        print("values appended")
        return 
    
    print("empty values passed")
    
async def filter_and_submit_report(usernames,message):
    
    username_entity = await client.get_entity(message.from_id.user_id)
    message_posted_by_username = username_entity.username
    if(message_posted_by_username is not None):
        for usr in usernames:
            if usr.lower()==message_posted_by_username.lower():
                message_text = message.message
                message_channel = await client.get_entity(message.peer_id.channel_id)
                message_date = message.date.date()
                        
                        # print("text: ",message_text)
                        # print("username: ",message_posted_by_username.username)
                        # print("channel: ",message_channel.username)
                        # print("date: ",message_date)
                        # print("-----------------")

                to_append = [f"{message_date}",usr,message_channel.username,message_text] 
                insert_spread(to_append)
                time.sleep(0.1)
    
        

async def t_scrapper(starting_link,ending_link,usernames=[]):
    print(starting_link,ending_link)
    channel_name,starting_message_id = starting_link.split("/")[-2],starting_link.split("/")[-1]
    ending_message_id = ending_link.split("/")[-1]

    await client(JoinChannelRequest(channel_name))
    channel_entity = await client.get_entity(channel_name)

    messages = client.iter_messages(
        channel_entity,
        limit=5000,
        min_id=int(starting_message_id),
        max_id=int(ending_message_id),
        reverse=True
    )
    count = 0

    async for message in messages:
        print(count)
        count+=1
        await filter_and_submit_report(usernames,message)

def get_links(url):
    gc = gspread.service_account(filename=config.spread_user_cred_json_file_addr)
    gsheet = gc.open_by_url(url)
    in_worksheet = gsheet.sheet1

    return in_worksheet.col_values(1) , in_worksheet.col_values(2)

    

async def main():

    starting_links,ending_links = get_links(config.fetch_spreadsheet_url)
    usernames = config.usernames
    
    for i in range(len(starting_links)):
        await t_scrapper(starting_links[i], ending_links[i],usernames)
    


with client:
    client.loop.run_until_complete(main())

  
