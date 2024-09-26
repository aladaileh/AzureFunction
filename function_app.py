import logging
import azure.functions as func
from requests import Session
from datetime import datetime , date, timedelta
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import os
import io
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp()



def upload_log_to_blob(function_name, log_stream):
        try:
            # Azure Blob Storage configurations
            AZURE_STORAGE_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
            BLOB_CONTAINER_NAME = f"{function_name}-log-container"  # Set the log container name
            BLOB_LOG_FILENAME = f"logs/log.log"  # Unique filename for the log
            
            # Create a BlobServiceClient using the connection string
            blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
            
            # Get the container client (create if doesn't exist)
            container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
            try:
                container_client.create_container()
            except Exception as e:
                pass
            
            # Create the blob client and upload the log
            blob_client = container_client.get_blob_client(BLOB_LOG_FILENAME)
            log_content = log_stream.getvalue() 
            blob_client.upload_blob(log_content, overwrite=True)
        
        except Exception as e:
            logging.error(f"Failed to upload logs to Blob Storage: {str(e)}")

def create_custom_logger(logger_name):
    log_stream = io.StringIO()  
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    stream_handler = logging.StreamHandler(log_stream)
    stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    if not logger.handlers:
        logger.addHandler(stream_handler)
    
    return logger, log_stream



def send_email(subject,body):
    smtp_server='smtp.gmail.com'
    port=587
    recipients = ['aadylih@gmail.com']
    password='dkenpftyzirukknz'
    sender='ahmadriad19971@gmail.com'
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






@app.schedule(schedule="0 0 10 * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
@app.blob_output(arg_name="outputblob", path="gasconsumption/inputs_df.csv", connection="AzureWebJobsStorage")
def EIAWeeklyReport(myTimer: func.TimerRequest, outputblob: func.Out[str]) :
    logger, log_stream = create_custom_logger('eia_weekly_logger')

    df = pd.DataFrame()

    try:
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
        res = r.content
        soup = BeautifulSoup(res, "html.parser")
    except Exception as e:
        logger.error(e)

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
            logger.info("Next Release information not found.")

    except Exception as e:
        logger.error(str(e))

    try:
            
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
    except Exception as e:
        logger.error(str(e))

    try:
        df = pd.concat([df, new_row], ignore_index=True)
        updated_csv_content = df.to_csv()
        outputblob.set(updated_csv_content)
        logger.info('EIA weekly report processed and saved to blob successfully.')
        upload_log_to_blob('eia-weeklyreport',log_stream=log_stream)
    except Exception as e:
        logger.error(str(e))
        subject="Error in Code (EIA_weeklyreport)"
        send_email(subject=subject,body=log_stream.getvalue())




@app.schedule(arg_name="timer3", schedule= "0 0 16 * *",run_on_startup=True,
              use_monitor=False)
@app.blob_output(arg_name="outputblob", path="gasconsumption/weather.csv",
                  connection="AzureWebJobsStorage")

def weather(timer3: func.TimerRequest, outputblob: func.Out[str]):
    logger, log_stream = create_custom_logger('weather_logger')
        
    try:
            
        url= requests.get('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/dubai/2024-08-31/2024-09-15?unitGroup=metric&include=days&key=U629JFB7PKJB9U4JKYDMS7DPF&contentType=json')
        response= url.json()
    except Exception as e:
        logger.error(f'colud not returen the data from the url coz {str(e)}')
    
    #%%%%%%%%%%
    try:
        days = response['days']
        df= pd.DataFrame(days)
        df.set_index(df['datetime'], inplace=True)
        weather = df[ 'temp']
    except Exception as e:
        logger.error(f'{str(e)}')
    
    try:
        for i in weather:
            if isinstance(i, float):      
                pass
    except Exception as e:
        logger.error(f'There is a problem with the data type')
         
    try:
        for i in pd.isnull(weather):
            if i:
                logger.error('There is a Null value')
                # raise ValueError('Null value detected in the data')
                
        
        
        weather.index= pd.to_datetime(weather.index)
        
        Commodity = 'Weather'
        location = 'UAE'
        Period = 'Daily'
        Source = 'visualcrossing'
        Senario = 'Base'
        Unit = 'Degree'
        Type = 'tempreture'
        
        
        #%%%%%%%%%%
        #generic
        
        # Create a dictionary with the data
        data = {
            'ValueDate': [weather.index[-1]],
            'AsOfDate': [weather.index[-1]],
            'Location': [location],
            'Commodity': [Commodity],
            'Period': [Period],
            'Source': [Source],
            'Senario': [Senario],
            'Timestamp': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            'Type': [Type],
            'Value': [weather[-1]],
            'SubType': [np.nan],
            'SubSubType': [np.nan],
            'Unit': [Unit],
            'HubType': [np.nan],
            'FreeText': [np.nan]
        }
    except Exception as e:
        logger.error(str(e))  
    try:
        new_row = pd.DataFrame(data)
        updated_csv_content = new_row.to_csv()
        outputblob.set(updated_csv_content)
        logger.info('Weather data processed and saved to blob successfully.')
    except Exception as e:
        logger.error(str(e))
        subject="Error in Code (Weather)"
        send_email(subject=subject,body=log_stream.getvalue())
    finally:
        upload_log_to_blob('weather',log_stream=log_stream)




