import os
import uuid
import logging
import asyncio


from aioca import camonitor
from aiokafka import AIOKafkaProducer
from epicscorelibs.ca.dbr import DBR_CHAR_BYTES
from ibex_non_ca_helpers.hex import dehex_decompress_and_dejson

from streaming_data_types.run_start_pl72 import serialise_pl72
from streaming_data_types.run_stop_6s4t import serialise_6s4t

logger = logging.getLogger("blah123")
logging.basicConfig(level=logging.INFO)


class RunStarter:
    def __init__(self, prefix, producer, topic):
        self.producer = None
        self.prefix = prefix
        self.blocks = []
        self.current_job_id = ""
        self.producer = producer
        self.topic = topic

    async def set_up_monitors(self):
        camonitor(f"{self.prefix}CS:BLOCKSERVER:BLOCKNAMES", callback=self._update_blocks,
                  datatype=DBR_CHAR_BYTES)
        camonitor(f"{self.prefix}DAE:RUNSTATE", callback=self._react_to_runstate_change,
                  all_updates=True, datatype=str)

    async def _update_blocks(self, value):
        logger.info(f"blocks_hexed: {value}")
        blocks_unhexed = dehex_decompress_and_dejson(bytes(value))
        logger.info(f"blocks_unhexed: {blocks_unhexed}")
        self.blocks = [f"{self.prefix}CS:SB:{x}" for x in blocks_unhexed]

    async def _react_to_runstate_change(self, value):
        logger.info(f"Runstate changed to {value}")

        if value == "BEGINNING":
            self.current_job_id = str(uuid.uuid4())
            await self.construct_and_send_runstart(self.current_job_id, self.prefix)
        elif value == "ENDING":
            await self.construct_and_send_runstop(self.current_job_id)

    async def construct_and_send_runstop(self, job_id: str):
        logger.info(f"Sending run stop with job_id: {job_id}")
        # TODO get run stop time here
        # serialise_6s4t(self.current_job_id)
        await self.producer.send(self.topic, f"run stop with job_id: {job_id}".encode())

    async def construct_and_send_runstart(self, job_id: str, prefix):
        logger.info(f"Sending run start with job_id: {job_id}")
        # TODO get start time as unix timestamp here
        # TODO construct nexus json here
        # serialise_pl72()
        await self.producer.send(self.topic, f"run start with job_id: {job_id}".encode())

async def set_up_producer(broker: str):
    producer = AIOKafkaProducer(bootstrap_servers=broker)
    await producer.start()
    return producer

def main():
    prefix = os.environ.get("MYPVPREFIX")

    broker = "localhost:19092"
    topic = "jacktestconfig"

    loop = asyncio.new_event_loop()
    producer = loop.run_until_complete(set_up_producer(broker))

    run_starter = RunStarter(prefix, producer, topic)
    loop.create_task(run_starter.set_up_monitors())
    loop.run_forever()


if __name__ == "__main__":
    main()
