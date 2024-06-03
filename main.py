import requests
import base64
import curlify
import logging
from logging import config
import time
import warnings
from credentials import PIPEDRIVE_API_KEY, SHIPCLOUD_API_KEY, EMAIL_PASS, EMAIL
import smtplib
from datetime import datetime
from io import BytesIO
import requests
import fitz  # PyMuPDF
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib
from PIL import Image

def configure_get_log():
    warnings.filterwarnings("ignore")

    config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s"
                },
                "slack_format": {
                    "format": "`[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]` %(message)s"
                },
            },
            "handlers": {
                "file": {
                    "class": "logging.FileHandler",
                    "formatter": "default",
                    "filename": "logs.log",
                },
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                },
            },
            
            "loggers": {
                "root": {
                    "level": logging.DEBUG,
                    "handlers": ["file", "console"],
                    "propagate": False,
                },
            },
        }
    )
    log = logging.getLogger("root")
    return log


log = configure_get_log()

def retry(max_retry_count, interval_sec):
    def decorator(func):
        def wrapper(*args, **kwargs):
            retry_count = 0
            while retry_count < max_retry_count:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retry_count += 1
                    log.error(f'{func.__name__} failed on attempt {retry_count}: {str(e)}')
                    if retry_count < max_retry_count:
                        log.info(f'Retrying {func.__name__} in {interval_sec} seconds...')
                        time.sleep(interval_sec)
            log.warning(f'{func.__name__} reached maximum retry count of {max_retry_count}.')
        return wrapper
    return decorator


class Pipedrive:
    pipedrive_api_key = PIPEDRIVE_API_KEY
    COMPANY_DOMAIN = 'drinkmamas'
    BASE_URL = f"https://{COMPANY_DOMAIN}.pipedrive.com"
    
    class Stages:
        ready_for_shipping = 45
        out_for_delivery = 46
        printed = 4
        delivered = 6

    class Endpoints:
        deals = "v1/deals"
        filters = "v1/filters"
        stages = "/api/v2/stages"
        update_deal = "/v1/deals/{id}"
        deal_fields = "/v1/dealFields"
    
    class CustomFields:
        # tracking_id = 'c1426df666fd49443e26bb36012119ec34c5a3a7'
        contact_person = '4038f1a6e2f4a6ac5b89dde211f4cecc8a7062bf'
        company = '4cb7ea08bfd7884d45f9356b4d2b892ddac23b70'
        street = '49e07c0099a5fa306b2f9d259b2bdb6904622120'
        housenumber = 'ac5b5083d27f16e83b8105a4f7a01a6f0af89b9d'
        postcode = 'aa0591d2fbbf1dd3b9d2e62bb49d110abc3597da'
        city = '1077b2967f82b8878320c7a6604c4c8f65bfda0c'
        shipcloud_id = 'c1426df666fd49443e26bb36012119ec34c5a3a7'



    @staticmethod
    def get(endpoint_extension: str, **query_params):
        query_params["api_token"] = Pipedrive.pipedrive_api_key
        url = f"{Pipedrive.BASE_URL}/{endpoint_extension}"
        response = requests.get(url, params=query_params)
        log.debug(f"CURL: {(curlify.to_curl(response.request))}")
        log.debug(f"RESPONSE: {response.text}")
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Could not fetch document. response = {response.text}, status_code = {response.status_code}")
            raise Exception(response.text)
    
    @staticmethod
    def post(endpoint_extension: str, data=None, json=None):
        query_params={"api_token": Pipedrive.pipedrive_api_key}
        url = f"{Pipedrive.BASE_URL}/{endpoint_extension}"
        response = requests.post(url, data=data, json=json, params=query_params)
        log.debug(f"CURL: {(curlify.to_curl(response.request))}")
        log.debug(f"RESPONSE: {response.text}")
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Could not fetch document. response = {response.text}, status_code = {response.status_code}")
            raise Exception(response.text)

    @staticmethod
    def put(endpoint_extension: str, data=None, json=None):
        query_params={"api_token": Pipedrive.pipedrive_api_key}
        url = f"{Pipedrive.BASE_URL}/{endpoint_extension}"
        response = requests.put(url, data=data, json=json, params=query_params)
        log.debug(f"CURL: {(curlify.to_curl(response.request))}")
        log.debug(f"RESPONSE: {response.text}")
        if response.status_code in [200, 201]:
            return response.json()
        else:
            log.error(f"Could not update document. response = {response.text}, status_code = {response.status_code}")
            raise Exception(response.text)
        
    @staticmethod
    @retry(max_retry_count=3, interval_sec=60)
    def get_deals_by_stage_id(stage_id: int):
        response =  Pipedrive.get(Pipedrive.Endpoints.deals, stage_id=stage_id)['data']
        return response or []

    @staticmethod
    def get_stages():
        response = Pipedrive.get(Pipedrive.Endpoints.stages)['data']
        return [{x["name"]: x["id"]} for x in response]
    
    @staticmethod
    def get_deal_fields():
        response =  Pipedrive.get(Pipedrive.Endpoints.deal_fields)['data']
        return [{x["name"]: x["key"]} for x in response]
    
    @staticmethod
    @retry(max_retry_count=3, interval_sec=10)
    def update_deal(deal_id, stage_id=None, tracking_id=None, shipcloud_id=None):
        log.info(f"""Updating deal : {deal_id}""")
        payload = {}
        if stage_id is not None:
            payload['stage_id'] = stage_id
        # if tracking_id is not None:
        #     payload[Pipedrive.CustomFields.tracking_id] = tracking_id
        if shipcloud_id is not None:
            payload[Pipedrive.CustomFields.shipcloud_id] = shipcloud_id
        response = Pipedrive.put(Pipedrive.Endpoints.update_deal.format(id=deal_id), json=payload)
        return response
        

class Shipcloud:
    API_KEY = SHIPCLOUD_API_KEY
    BASE_URL = "https://api.shipcloud.io/v1"

    class Endpoints:
        create_shipment = "shipments"
        track_parcel = "trackers"

    class Status:
        out_for_delivery = "out_for_delivery"
        delivered = 'delivered'

    @staticmethod
    def get(endpoint_extension: str, **query_params):
        header = {
            "Authorization": f"Basic {base64.b64encode(Shipcloud.API_KEY.encode()).decode()}",
            "Content-Type": "application/json"
        }
        url = f"{Shipcloud.BASE_URL}/{endpoint_extension}"
        response = requests.get(url, params=query_params, headers=header)
        log.debug(f"CURL: {(curlify.to_curl(response.request))}")
        log.debug(f"RESPONSE: {response.text}")
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Could not fetch document. response = {response.text}, status_code = {response.status_code}")
            raise Exception(response.text)
    
    @staticmethod
    def post(endpoint_extension: str, data=None, json=None):
        header = {
            "Authorization": f"Basic {base64.b64encode(Shipcloud.API_KEY.encode()).decode()}",
            "Content-Type": "application/json"
        }
        url = f"{Shipcloud.BASE_URL}/{endpoint_extension}"
        response = requests.post(url, data=data, json=json, headers=header)
        log.debug(f"CURL: {(curlify.to_curl(response.request))}")
        log.debug(f"RESPONSE: {response.text}")
        if response.status_code in [200, 201]:
            return response.json()
        else:
            log.error(f"Could not post document. response = {response.__dict__}, status_code = {response.status_code}, data = {data}")
            raise Exception(response.text)
        
    @staticmethod
    @retry(max_retry_count=3, interval_sec=20)
    def create_shipment_request(pipedrive_id="", company="", first_name="", last_name="", street="", street_no="", zip_code="", city="",email="logistics@brandgarage.de", country="DE"):
        payload = {
            "to": {
                'company': company,
                'first_name': first_name,
                'last_name': last_name,
                'street': street,
                'street_no': street_no,
                'zip_code': zip_code,
                'city': city,
                'country': country,
            },
            "package": {
                'length': 50.0,
                'width': 25.0,
                'height': 8.0,
                'weight': 3.0,
                'type': 'parcel'
            },
            "carrier": "iloxx",
            "service": "standard",
            "reference_number": pipedrive_id,
            "notification_email": email,
            "create_shipping_label": True
        }

        response = Shipcloud.post(Shipcloud.Endpoints.create_shipment, json=payload)
        log.info(response)
        return response
    

    @staticmethod
    @retry(max_retry_count=3, interval_sec=10)
    def get_shipments(shipment_id=None):
        if not shipment_id:
            response = Shipcloud.get(Shipcloud.Endpoints.create_shipment)
            return response
        else:
            response = Shipcloud.get(Shipcloud.Endpoints.create_shipment, id=shipment_id)
            for ship in response['shipments']:
                if ship['id'] == shipment_id:
                    return ship
            return []
        
class Emailer:
    def __init__(self, pdf_urls, to_email='logistics@brandgarage.de'):
        self.pdf_urls = pdf_urls
        self.to_email = to_email

    def pdf_url_to_image(self, url, resolution=300):
        response = requests.get(url)
        response.raise_for_status()
        pdf_bytes = response.content
        doc = fitz.open("pdf", pdf_bytes)
        page = doc.load_page(0)
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # Increase resolution by scaling the pixmap
        img = Image.open(BytesIO(pix.tobytes()))
        return img

    def create_collage(self):
        images = [self.pdf_url_to_image(url, resolution=300) for url in self.pdf_urls]

        # Determine the number of collages needed
        num_collages = (len(images) + 3) // 4

        collages = []
        for i in range(num_collages):
            collage_images = images[i*4 : (i+1)*4]
            image_width, image_height = collage_images[0].size
            collage_width = 2 * image_width
            collage_height = 2 * image_height
            collage = Image.new('RGB', (collage_width, collage_height), 'white')
            for j, img in enumerate(collage_images):
                x = j % 2 * image_width
                y = j // 2 * image_height
                collage.paste(img, (x, y))

            collages.append(collage)

        return collages

    def send_email(self):

        smtp_server = 'smtp.gmail.com'
        port = 465  # SSL port
        current_date = datetime.now().strftime('%d.%m.%Y')
        subject = f'Ihre Versandetiketten für {current_date}'
        body = f"""
        Hallo,

        Bitte finden Sie das Etiketten-PDF im Anhang.

        {self.pdf_urls}

        Vielen Dank.
        Kumaresh Dey,
        Softwarehouse
        softwarehouse@myyahoo.com
        """
        message = MIMEMultipart()
        message['From'] = EMAIL
        message['To'] = self.to_email
        message['Subject'] = subject
        message.attach(MIMEText(body, 'plain'))

        # Create collages from the PDF pages
        collages = self.create_collage()

        # Save the collages into a single PDF file
        output_pdf_bytes = BytesIO()
        with fitz.open() as doc:
            for collage in collages:
                img_bytes = BytesIO()
                collage.save(img_bytes, format='PDF')
                img_bytes.seek(0)
                img_doc = fitz.open("pdf", img_bytes.getvalue())
                doc.insert_pdf(img_doc)

            doc.save(output_pdf_bytes)

        # Attach the PDF to the email
        attachment = MIMEApplication(output_pdf_bytes.getvalue())
        attachment.add_header('Content-Disposition', 'attachment', filename=f'versandetiketten_{current_date}.pdf')
        message.attach(attachment)

        try:
            server = smtplib.SMTP_SSL(smtp_server, port)
            server.login(EMAIL, EMAIL_PASS)
            server.sendmail(EMAIL, message['To'] , message.as_string())
            log.info('Email sent successfully')
        except smtplib.SMTPAuthenticationError as auth_error:
            log.error(f'SMTP Authentication Error: {auth_error}')
            raise smtplib.SMTPAuthenticationError()
        except Exception as e:
            log.error(f'Error: {e}')
            raise Exception()
        finally:
            server.quit()


def update_delivery_statuses():
    deals_out_for_delivery = Pipedrive.get_deals_by_stage_id(Pipedrive.Stages.out_for_delivery)
    deals_printed = Pipedrive.get_deals_by_stage_id(Pipedrive.Stages.printed)
    for deal in deals_out_for_delivery:
        if deal[Pipedrive.CustomFields.shipcloud_id]:
            log.info(f"""Checking delivery status for : {deal["title"]}""")
            shipment = Shipcloud.get_shipments(shipment_id=deal[Pipedrive.CustomFields.shipcloud_id])
            for event in shipment['packages'][0]['tracking_events']:
                if event['status'] == Shipcloud.Status.delivered:
                    update_deal = Pipedrive.update_deal(deal_id=deal['id'], stage_id=Pipedrive.Stages.delivered)
                    break
    for deal in deals_printed:
        if deal[Pipedrive.CustomFields.shipcloud_id]:
            log.info(f"""Checking delivery status for : {deal["title"]}""")
            shipment = Shipcloud.get_shipments(shipment_id=deal[Pipedrive.CustomFields.shipcloud_id])
            for event in shipment['packages'][0]['tracking_events']:
                if event['status'] == Shipcloud.Status.delivered:
                    update_deal = Pipedrive.update_deal(deal_id=deal['id'], stage_id=Pipedrive.Stages.delivered)
                    break
                if event['status'] == Shipcloud.Status.out_for_delivery:
                    update_deal = Pipedrive.update_deal(deal_id=deal['id'], stage_id=Pipedrive.Stages.out_for_delivery)
    return True


def create_shipments():
    links = []
    deals = Pipedrive.get_deals_by_stage_id(Pipedrive.Stages.ready_for_shipping)
    for deal in deals:
        log.info(f"""Creating shipment for : {deal["title"]}""")
        email = ''
        if deal['person_id']['email']:
            email = deal['person_id']['email'][0].get('value')
        tracking_details = Shipcloud.create_shipment_request(
            pipedrive_id=deal['id'],
            company=deal[Pipedrive.CustomFields.company], 
            first_name=deal[Pipedrive.CustomFields.contact_person] or deal['person_id']['name'],
            street=deal[Pipedrive.CustomFields.street], 
            street_no=deal[Pipedrive.CustomFields.housenumber],
            zip_code=deal[Pipedrive.CustomFields.postcode], 
            city=deal[Pipedrive.CustomFields.city],
            email=email
        )
        if tracking_details:
            tracking_id = tracking_details['carrier_tracking_no']
            shipcloud_id = tracking_details['id']
            links.append(tracking_details['label_url'])
            update_deal = Pipedrive.update_deal(deal_id=deal['id'], stage_id=Pipedrive.Stages.printed, tracking_id=tracking_id, shipcloud_id=shipcloud_id)
        else:
            log.error(f"""Creating shipment failed for: {deal["title"]}""")
    return links


def run_pipeline():
    log.info("Running: update_delivery_statuses")
    result = update_delivery_statuses()
    log.info(f'update_delivery_statuses completed. result = {result}')
    log.info("Running: create_shipments")
    result = create_shipments()
    log.info(f'create_shipments completed. result = True')
    if result:
        Emailer(result).send_email()
    