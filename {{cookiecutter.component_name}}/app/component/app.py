from app.component.settings import *
import asyncio
from photonpump import connect, exceptions
import json
import functools
from pymongo import MongoClient
import uuid
import requests


def run_in_executor(f):
    """
    wraps a blocking (non-asyncio) function to execute it in the loop as if it were an async func
    """
    @functools.wraps(f)
    def inner(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(None, functools.partial(f, *args, **kwargs))
    return inner


@run_in_executor
def create_response(event):
    raise NotImplementedError


@run_in_executor
def post_to_dialogue_stream(event, result_text):
    event_data = json.loads(event.data)
    headers = {
        "ES-EventType": "response_created",  # default for components responding to a query
        "ES-EventId": str(uuid.uuid1())
    }
    requests.post(
        "http://%s:%s/streams/dialogue" % (EVENT_STORE_URL, EVENT_STORE_HTTP_PORT),
        headers=headers,
        json={"event_id": event_data["event_id"], "response": result_text}
    )


def meets_criteria(event)->bool:
    """
    :param data object from event:
    :return: bool based on whether this component should process the event object
    """
    raise NotImplementedError(event)


async def create_subscription(subscription_name, stream_name, conn):
    await conn.create_subscription(subscription_name, stream_name)


async def aggregate_fn():
    _loop = asyncio.get_event_loop()
    async with connect(
            host=EVENT_STORE_URL,
            port=EVENT_STORE_TCP_PORT,
            username=EVENT_STORE_USER,
            password=EVENT_STORE_PASS,
            loop=_loop
    ) as c:
        await c.connect()
        try:
            await create_subscription("{{cookiecutter.component_name}}", "dialogue", c)
        except exceptions.SubscriptionCreationFailed as e:
            if e.message.find("'{{cookiecutter.component_name}}' already exists."):
                log.info("{{cookiecutter.component_name}} dialogue subscription found.")
            else:
                log.exception(e)
        dialogue_stream = await c.connect_subscription("{{cookiecutter.component_name}}", "dialogue")
        async for event in dialogue_stream.events:
            if meets_criteria(event):
                log.debug("aggregate_fn() responding to: %s" % json.dumps(event))
                try:
                    await post_to_dialogue_stream(event, create_response(event))
                    await dialogue_stream.ack(event)
                except Exception as e:
                    log.exception(e)
            else:
                await dialogue_stream.ack(event)


if __name__ == "__main__":
    asyncio.set_event_loop(asyncio.new_event_loop())
    mainloop = asyncio.get_event_loop()
    mainloop.run_until_complete(aggregate_fn())
