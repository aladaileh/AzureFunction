import logging
import azure.functions as func
from requests import Session
from datetime import datetime , date, timedelta
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
# import os
# import pyocr
# import getpass
# from PIL import Image
# import re
# from azure.storage.blob import BlobServiceClient
# import io
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


app = func.FunctionApp()

# @app.schedule(schedule="0 0 16 * * *", arg_name="myTimer", run_on_startup=True,
#               use_monitor=False) 
# @app.blob_output(arg_name="outputblob", path="gasconsumption/inputs_df.csv", connection="AzureWebJobsStorage")
# def AemoActualeFlow(myTimer: func.TimerRequest, outputblob: func.Out[str]) :
   
#     def send_email(smtp_server,port,sender,password,recipients,subject,body):
#         msg=MIMEMultipart()
#         msg['From']= sender
#         msg['To']=','.join(recipients)
#         msg['Subject']=subject
#         msg.attach(MIMEText(body,'Plain'))
#         server=smtplib.SMTP(smtp_server,port)
#         server.starttls()
#         server.login(sender, password)
#         server.send_message(msg)
#         server.quit()  
            
#     smtp_server='smtp.gmail.com'
#     port=587
#     recipients = ['aadylih@gmail.com']
#     password='dkenpftyzirukknz'
#     sender='ahmadriad19971@gmail.com'
#     subject="Code (AemoNomsAndForecastFlow)"
    



#     if myTimer.past_due:
#         logging.info('The timer is past due!')

#     logging.info('Python timer trigger function executed.')

#     try:

#         class GAS:
#             def __init__(self):
#                 self.apiurl= "https://www.aemo.com.au/aemo/api/v1/GasBBReporting/DailyProductionAndFlow"
#                 self.headers= { 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'}
#                 self.session= Session()
#                 self.session.headers.update(self.headers)
#             def AllDailyProduction(self):
#                 url=self.apiurl
#                 r= self.session.get(url)
#                 data= r.json()['data']
#                 logging.error(data)
#                 return data
     
    
#         gas=GAS()
#         d= gas.AllDailyProduction()

#         Daily_production=pd.DataFrame([i.values() for i in d['DailyProductionAndFlowList']],columns= list(d['DailyProductionAndFlowList'][0]))
#         logging.info(Daily_production)
#         Daily_production2 = Daily_production.copy()

#         Daily_production2['Location'] = Daily_production2['FacilityName'] + '-' + Daily_production2['LocationName']
#         Daily_production2['Commodity']= 'gas'
#         Daily_production2['Period']='daily'
#         Daily_production2['Source']='aemo'
#         Daily_production2['Senario']='Base'
#         Daily_production2['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#         Daily_production2['Type']= [['Demand','Supply','TransferIn','TransferOut']] * len(Daily_production2)

#         Daily_production2 = Daily_production2.explode(['Type']) #.fillna('')

#         Daily_production2['Value'] = np.where(Daily_production2['Type'] == 'Demand', Daily_production2['Demand'], np.nan)
#         Daily_production2['Value'] = np.where(Daily_production2['Type'] == 'Supply', Daily_production2['Supply'], Daily_production2['Value'])
#         Daily_production2['Value'] = np.where(Daily_production2['Type'] == 'TransferIn', Daily_production2['TransferIn'], Daily_production2['Value'])
#         Daily_production2['Value'] = np.where(Daily_production2['Type'] == 'TransferOut', Daily_production2['TransferOut'], Daily_production2['Value'])

#         AemoActualFlow=Daily_production2.drop(['FacilityId','FacilityName','LocationId','LocationName','Demand','Supply','TransferIn','TransferOut','HeldInStorage'],axis=1)
#         AemoActualFlow.rename(columns={'LastUpdated':'AsOfDate','GasDate':'ValueDate'}, inplace=True)

#         AemoActualFlow['SubType']=np.nan
#         AemoActualFlow['SubSubTybe']=np.nan
#         AemoActualFlow['Unit']=np.nan
#         AemoActualFlow['HubType']=np.nan
#         AemoActualFlow['FreeText']=np.nan
#         # AemoActualFlow.to_csv(r'D:\work\Data\AemoActualFlow.csv', index=False)
#         # connection_string = "DefaultEndpointsProtocol=https;AccountName=gasdata;AccountKey=3uSxBm73c2YLzhO6NR8JJT0yY8HfipmdyO/P3w3womgdtq32b9/OnHk8xylZ+gvE3juHgxDvILAn+AStcfMLpg==;EndpointSuffix=core.windows.net"
#         # blob_service_client = BlobServiceClient.from_connection_string(connection_string)
#         # container_name = "gasconsumption"
#         # blob_name = "inputs_df.csv"
#         # blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
#         updated_csv_content = AemoActualFlow.to_csv()
#         # with tempfile.NamedTemporaryFile(delete=False) as temp_file:
#         #     temp_file.write(updated_csv_content.encode())
#         #     temp_file.seek(0)
#         #     blob_client.upload_blob(temp_file, overwrite=True)
#         outputblob.set(updated_csv_content)

#         logging.info("Changes saved successfully!")
#         logging.info(f"CSV content: {AemoActualFlow}")
#         body= "Changes saved successfully!"
#         # send_email(smtp_server,port,sender,password,recipients,subject,body)
#     except Exception as e:
#         logging.error(f"Error accessing CSV file: {e}")








# @app.schedule(arg_name='timer2', schedule= '0 0 16 * * *',run_on_startup=True,
#               use_monitor=False )
# @app.blob_output(arg_name="outputblob1", path="gasconsumption/AemoNomsAndForcastFlow.csv",
#                   connection="AzureWebJobsStorage")

# def AemoNomsAndForcastFlow(timer2: func.TimerRequest, outputblob1: func.Out[str]):

#     def send_email(smtp_server,port,sender,password,recipients,subject,body):
#         msg=MIMEMultipart()
#         msg['From']= sender
#         msg['To']=','.join(recipients)
#         msg['Subject']=subject
#         msg.attach(MIMEText(body,'Plain'))
#         server=smtplib.SMTP(smtp_server,port)
#         server.starttls()
#         server.login(sender, password)
#         server.send_message(msg)
#         server.quit()
            
#     smtp_server='smtp.gmail.com'
#     port=587
#     recipients = ['aadylih@gmail.com']
#     password='dkenpftyzirukknz'
#     sender='ahmadriad19971@gmail.com'
#     subject="Error in Code (AemoNomsAndForecastFlow)"

#     #%%
    
#     try:
        

#         class GAS:
#             def __init__(self):
#                 gas_date_from = datetime.today() 
#                 gas_date_from_str = gas_date_from.strftime("%d/%m/%Y")

#                 self.apiurl= f"https://www.aemo.com.au/aemo/api/v1/GasBBReporting/NominationsAndForecasts"
#                 self.headers= { 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'}
#                 self.session= Session()
#                 self.session.headers.update(self.headers)

#             def AllDailyProduction(self):
#         #     url=self.apiurl
#         #     r= self.session.get(url)
#         #     data= r.json()['data']
#         #     return data
#         # def LastMonthProduction(self):
#                 gas_date_from = datetime.today()
#                 gas_date_to = gas_date_from + timedelta(days=30)
#                 gas_date_from_str = gas_date_from.strftime("%Y/%m/%d")
#                 gas_date_to_str = gas_date_to.strftime("%Y/%m/%d")
#                 url=self.apiurl+f'?FromGasDate={gas_date_from_str}&ToGasDate={gas_date_to_str}'
#                 r= self.session.get(url) 
#                 data= r.json()['data']
#                 return data
    
#         gas=GAS()
#         d= gas.AllDailyProduction()

#         Daily_production=pd.DataFrame([i.values() for i in d['NominationsAndForecastsList']],columns= list(d['NominationsAndForecastsList'][0]))

#     except Exception as e:
        
#         raise Exception(send_email(smtp_server, port, sender, password, recipients,subject, str(e)))

#     #%%
#     try:
#         Daily_production2 = Daily_production.copy()

#         Daily_production2['Location'] = Daily_production2['FacilityName'] + '-' + Daily_production2['LocationName']
#         Daily_production2['Commodity']= 'gas'
#         Daily_production2['Period']='daily'
#         Daily_production2['Source']='aemo'
#         Daily_production2['Senario']='Base'
#         Daily_production2['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#         Daily_production2['Type']= [['Demand','Supply','TransferIn','TransferOut']] * len(Daily_production2)


#         Daily_production2 = Daily_production2.explode(['Type']) #.fillna('')


#         Daily_production2['Value'] = np.where(Daily_production2['Type'] == 'Demand', Daily_production2['Demand'], np.nan)
#         Daily_production2['Value'] = np.where(Daily_production2['Type'] == 'Supply', Daily_production2['Supply'], Daily_production2['Value'])
#         Daily_production2['Value'] = np.where(Daily_production2['Type'] == 'TransferIn', Daily_production2['TransferIn'], Daily_production2['Value'])
#         Daily_production2['Value'] = np.where(Daily_production2['Type'] == 'TransferOut', Daily_production2['TransferOut'], Daily_production2['Value'])


#         AemoActualFlow=Daily_production2.drop(['FacilityId','FacilityName','LocationId','LocationName','Demand','Supply','TransferIn','TransferOut'],axis=1)
#         AemoActualFlow.rename(columns={'LastUpdated':'AsOfDate','GasDate':'ValueDate'}, inplace=True)



#         AemoActualFlow['SubType']=np.nan
#         AemoActualFlow['SubSubTybe']=np.nan
#         AemoActualFlow['Unit']=np.nan
#         AemoActualFlow['HubType']=np.nan
#         AemoActualFlow['FreeText']=np.nan

#         updated_csv_content = AemoActualFlow.to_csv()

#         outputblob1.set(updated_csv_content)

#         # AemoActualFlow.to_csv(r'D:\work\Data\AemoNomsAndForecastedFlow.csv', index=False)
#         logging.info("AemoNomsAndForecastedFlow saved successfully!")

#     except Exception as e:
        
#         raise Exception(send_email(smtp_server, port, sender, password, recipients,subject, str(e)))
        




@app.schedule(arg_name="timer3", schedule= "0 0 16 * *",run_on_startup=True,
              use_monitor=False)
@app.blob_output(arg_name="outputblob2", path="gasconsumption/KHNPAvialability.csv",
                  connection="AzureWebJobsStorage")

def KHNPAvialability(timer3: func.TimerRequest, outputblob2: func.Out[str]):

    def send_email(smtp_server,port,sender,password,recipients,subject,body):
        msg=MIMEMultipart()
        msg['From']= sender
        msg['To']=','.join(recipients)
        msg['Subject']=subject
        msg.attach(MIMEText(body,'Plain'))
        server=smtplib.SMTP(smtp_server,port)
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)
        server.quit()
        
    smtp_server='smtp.gmail.com'
    port=587
    recipients = ['aadylih@gmail.com']
    password='dkenpftyzirukknz'
    sender='ahmadriad19971@gmail.com'
    subject="Error in Code (KHNPAvailability)"

    def get_translated_text(text):
        """
        Returns the translated text.
        """

        URL = "https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=en&dt=t&q={text}".format(
            text=text
        )

        response = requests.get(URL)

        if response.status_code == 200:
            json_response = response.json()
            translated_text = json_response[0][0][0]

            return translated_text
        else:
            raise text #Exception("Failed to translate text: {}".format(response.status_code))
    #%%%

    def take_and_leave_one(input_list):
        result = []
        for i in range(0, len(input_list), 2):
            result.append(input_list[i])
        return result
    def leave_and_take_one(input_list):
        result = []
        for i in range(1, len(input_list), 2):
            result.append(input_list[i])
        return result

    #%%%

    addresses = [
        "https://www.khnp.co.kr/eng/realTimeBranch.do?key=517&branchCd=BR0302",
        "https://www.khnp.co.kr/eng/realTimeBranch.do?key=518&branchCd=BR0303",
        "https://www.khnp.co.kr/eng/realTimeBranch.do?key=519&branchCd=BR0305",
        "https://www.khnp.co.kr/eng/realTimeBranch.do?key=520&branchCd=BR0304",
        "https://www.khnp.co.kr/eng/realTimeBranch.do?key=521&branchCd=BR0312"
    ]
    def return_response_with_encoding(url):
        response = requests.get(url, headers={
                        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
                    }, verify=False)#, encoding=encoding)
        return response.content

    response = [return_response_with_encoding(url) for url in addresses]

    response_string = [r.decode("utf-8") for r in response]

    soup = [BeautifulSoup(res, "html.parser") for res in response_string]
    #%%%%
    plant_name = [s.select_one("div [class='sub_title'] ").text.strip() for s in soup]
    comment =[ s.select_one("div[class='point_con_box'] p").text.strip() for s in soup]
    unit_numbers = [[i.text.strip() for i in table.select("tbody tr th")] for table in [soup[i].select_one("div [class='table_scroll']  table ") for i in range(len(soup))]]
    OutputGeneration = [take_and_leave_one([i.text.strip() for i in table.select("tbody tr td")] )for table in [soup[i].select_one("div [class='table_scroll']  table ") for i in range(len(soup))]]
    TimeOfInspection = [leave_and_take_one([i.text.strip() for i in table.select("tbody tr td")] )for table in [soup[i].select_one("div [class='table_scroll']  table ") for i in range(len(soup))]]
    Location = [f'{plant_name[i]} Unit {y}' for i in range(len(plant_name)) for y in unit_numbers[i]]
    # Location=[]
    # for i in range(len(plant_name)):
    #     p= plant_name[i]
    #     for y in unit_numbers[i]:
    #         loc=  f'{p} Unit {y}'
    #         Location.append(loc)

    comment[0]= get_translated_text(comment[0])
    comment[3]= get_translated_text(comment[3])

    #%%

    try:
            
        curvesToUpload = {
            "Location":Location ,
            "AsOfDate":  str(date.today()) ,
            "ValueDate":TimeOfInspection ,
            "Value":OutputGeneration


            }
        
        curvesToUpload["ValueDate"] = [str(item) for sublist in curvesToUpload["ValueDate"] for item in sublist]
        curvesToUpload["Value"] = [float(item.replace(',', '')) for sublist in curvesToUpload["Value"] for item in sublist]
        # curvesToUpload["ValueDate"] = [convert_datetime_string(i) for i in curvesToUpload["ValueDate"] ]
        
        
        
        KHNPAvailability=pd.DataFrame(curvesToUpload)
        # KHNPAvailability= KHNPAvailability.explode(['Value','ValueDate'])
        KHNPAvailability['Commodity']= 'Power'
        KHNPAvailability['Period']='daily'
        KHNPAvailability['Source']='KHNP'
        KHNPAvailability['Senario']='Base'
        KHNPAvailability['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        KHNPAvailability['Type']= 'Generation'
        KHNPAvailability['SubType']='Availability'
        KHNPAvailability['SubSubType']=np.nan
        KHNPAvailability['Unit']='MW'
        KHNPAvailability['HubType']='Generation Unit'
        KHNPAvailability['FreeText']=np.nan
        column_names=['ValueDate','AsOfDate','Location','Commodity','Period','Source','Senario','Timestamp','Type','Value','SubType','SubSubType','Unit','HubType','FreeText']
        KHNPAvailability=KHNPAvailability[column_names]
        updated_csv_content = KHNPAvailability.to_csv()

        outputblob2.set(updated_csv_content) 
        logging.info("KHNPAvailability saved successfully!")
   
    except Exception as e:
        
        raise Exception(send_email(smtp_server, port, sender, password, recipients,subject, str(e)))




@app.schedule(schedule="0 0 16 * 1", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
@app.blob_output(arg_name="outputblob", path="gasconsumption/inputs_df.csv", connection="AzureWebJobsStorage")
def EIAWeeklyReport(myTimer: func.TimerRequest, outputblob: func.Out[str]) :
        
    # Initialize an empty DataFrame
    df = pd.DataFrame()

    # Set up the session and headers
    url = "https://ir.eia.gov/ngs/ngs.html"
    headers = {
        "Age": "20",
        "Cache-Control": "public, max-age=30",
        "Date": "Tue, 23 Jul 2024 11:30:02 GMT",
        "Via": "1.1 16f38d6df135d34d67fe44df60d91ab4.cloudfront.net (CloudFront)",
        "X-Amz-Cf-Id": "qQVBzdm5Jed8YQ_fOKtHolyveGuPEZ15M5igonZ-p672w1vYV-Oe3A==",
        "X-Amz-Cf-Pop": "LHR61-P1",
        "X-Cache": "Hit from cloudfront"
    }
    session = Session()
    session.headers.update(headers)
    r = session.get(url)

    try:
        res = r.content
        soup = BeautifulSoup(res, "html.parser")
    except Exception as e:
        logging.error(e)

    try:
        table = soup.find('table')
        last_row = table.find_all('tr')[-2]
        first_row = table.find_all('tr')[2]
        value = last_row.find_all('td')[1].get_text()
        Value = value.replace(',', '')
        valuedate = first_row.find_all('td')[2].get_text().split()[2].strip("()")
        date_obj = datetime.strptime(valuedate, "%m/%d/%y")
        div_element = soup.find('div', class_='report_header')
        text_content = div_element.get_text()
        next_release_index = text_content.find("Next Release:")
        if next_release_index != -1:
            next_release_date = text_content[next_release_index:].split("Next Release:")[1].strip()
            AsOfDate = next_release_date.split(' ')[0] + ' ' + next_release_date.split(' ')[1] + ' ' + next_release_date.split(' ')[2]
            AsOfDate=datetime.strptime(AsOfDate, '%B %d, %Y')
            
        else:
            print("Next Release information not found.")

    except Exception as e:
        logging.error(str(e))

    # Prepare the data
    Value_Date = date_obj
    
    Commodity = 'Gas'
    location = 'USA'
    Period = 'Weekly'
    Source = 'EIA'
    Senario = 'Base'
    Unit = 'Bcf'
    Type = 'Storage'


    #%%%%%%%%%%
    #generic

    # Create a dictionary with the data
    data = {
        'ValueDate': [Value_Date],
        'AsOfDate': [AsOfDate],
        'Location': [location],
        'Commodity': [Commodity],
        'Period': [Period],
        'Source': [Source],
        'Senario': [Senario],
        'Timestamp': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        'Type': [Type],
        'Value': [Value],
        'SubType': [np.nan],
        'SubSubType': [np.nan],
        'Unit': [Unit],
        'HubType': [np.nan],
        'FreeText': [np.nan]
    }

    # Create a DataFrame from the dictionary
    new_row = pd.DataFrame(data)

    # Concatenate the new row to the existing DataFrame
    df = pd.concat([df, new_row], ignore_index=True)
    updated_csv_content = df.to_csv()
    outputblob.set(updated_csv_content)

