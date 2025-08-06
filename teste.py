import asyncio
import requests
import os
import ssl
import json

from aiokafka import AIOKafkaConsumer

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP").split(",")
apikey = os.environ.get("IBM_API_KEY")

iam_token = None
consumer = None
restart_consumer_event = asyncio.Event() 

async def get_iam_token():
    global iam_token
    resp = requests.post(
        "https://iam.cloud.ibm.com/identity/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
            "apikey": apikey
        }
    )
    resp.raise_for_status()
    iam_token = resp.json()["access_token"]

async def renew_token_loop():
    while True:
        await asyncio.sleep(3000)  
        await get_iam_token()
        restart_consumer_event.set()

async def consume_loop():
    global consumer
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED

    while True:
        
        consumer = AIOKafkaConsumer(
            'cloud-audit-logs',
            bootstrap_servers=bootstrap_servers,
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username='token',
            sasl_plain_password=iam_token,
            ssl_context=ssl_context,
            group_id='RG-Projeto-Remediacao',
            client_id='proj-remediacao',
            auto_offset_reset='earliest'
        )
    
        await consumer.start()
        try:
            async for msg in consumer:
                log_data = json.loads(msg.value.decode("utf-8"))


                log_source = log_data.get("data", {}).get('logSourceCRN', "")
                host_address = log_data.get("data", {}).get("target", {}).get("host", {}).get("address", "")
                name = log_data.get("data", {}).get("target", {}).get("name", "")
            
                url = f"https://{host_address}/{name}"

                headers = {"Authorization": f"bearer {iam_token}", "ibm-service-instance-id": log_source}

                delete = requests.delete(url=url,headers=headers)

                if restart_consumer_event.is_set():
                    restart_consumer_event.clear()
                    break

            

        finally:
            await consumer.stop()

def main(params):
    asyncio.run(asyncio.wait_for(handler(params), timeout=120))
    return {"status": "consumer started"}

async def handler(params):
    await get_iam_token()
    asyncio.create_task(renew_token_loop())
    await consume_loop()
