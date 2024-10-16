import asyncio
from pyzeebe import ZeebeWorker, create_insecure_channel
from pyzeebe import ZeebeTaskRouter, ZeebeClient, Job

router = ZeebeTaskRouter()
client = None 

@router.task(task_type="confirmOrder")
async def confirmOrder(job: Job, *args, **kwargs):
    print("confirmOrder Job")
    product = job.variables.get("order")
    print("product :", product)
    await client.publish_message(
        name="item-order", 
        correlation_key="item-order-"+product, 
        variables={ "product" : product })
    print("ok")
    return

@router.task(task_type="checkStock", single_value=True, variable_name="stock")
async def checkStock(job: Job, *args, **kwargs) -> int:
    print("checkStock Job")
    product : str = job.variables.get("product")
    print("product :", product)
    if (product.find("meja") != -1):
        print("current stock : 1")
        return 1
    else:
        print("current stock : 0")
        return 0
    
@router.task(task_type="notifySoldOut")
async def notifySoldOut(job: Job, *args, **kwargs):
    print("notifySoldOut Job")
    product = job.variables.get("product")
    print("product :", product)
    await client.publish_message(
        name="sold_out", 
        correlation_key=product)
    print("ok")
    return

@router.task(task_type="notifySuccessful")
async def notifySuccessful(job: Job, *args, **kwargs):
    print("notifySuccessful Job")
    product = job.variables.get("product")
    print("product :", product)
    await client.publish_message(
        name="successful", 
        correlation_key=product)
    print("ok")
    return

async def main():
    global client
    channel = create_insecure_channel()
    worker = ZeebeWorker(channel)
    client = ZeebeClient(channel)
    worker.include_router(router)
    await worker.work()

asyncio.run(main())