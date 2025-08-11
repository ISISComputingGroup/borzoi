import os
import uuid
import logging
import asyncio


from aioca import run, camonitor, caget
from epicscorelibs.ca.dbr import DBR_CHAR_BYTES
from ibex_non_ca_helpers.hex import dehex_decompress_and_dejson

from streaming_data_types.run_start_pl72 import serialise_pl72
from streaming_data_types.run_stop_6s4t import serialise_6s4t

logger = logging.getLogger("blah123")
logging.basicConfig(level=logging.INFO)


class RunStarter:

    def __init__(self, prefix, loop):
        self.producer = None
        self.prefix = prefix
        self.blocks = []
        self.current_job_id = ""

        loop.create_task(self.set_up_monitors())
        loop.run_forever()

    async def set_up_monitors(self):
        camonitor(f"{self.prefix}CS:BLOCKSERVER:BLOCKNAMES", callback=self.update_blocks,
                  datatype=DBR_CHAR_BYTES)
        camonitor(f"{self.prefix}DAE:RUNSTATE", callback=self.react_to_runstate_change,
                  all_updates=True, datatype=str)

    async def update_blocks(self, value):
        logger.info(f"blocks_hexed: {value}")
        blocks_unhexed = dehex_decompress_and_dejson(bytes(value))
        logger.info(f"blocks_unhexed: {blocks_unhexed}")
        self.blocks = [f"{self.prefix}CS:SB:{x}" for x in blocks_unhexed]

    async def react_to_runstate_change(self, value):
        logger.info(f"Runstate changed to {value}")

        if value == "BEGINNING":
            self.current_job_id = str(uuid.uuid4())
            await self.construct_and_send_runstart(self.current_job_id, self.prefix)
        elif value == "ENDING":
            self.construct_and_send_runstop(self.current_job_id)

    def construct_and_send_runstop(self, job_id: str):
        logger.info(f"Sending run stop with job_id: {job_id}")
        # TODO get run stop time here
        # serialise_6s4t(self.current_job_id)

    async def construct_and_send_runstart(self, job_id: str, prefix):
        logger.info(f"Sending run start with job_id: {job_id}")
        # TODO get start time as unix timestamp here
        # TODO construct nexus json here
        # serialise_pl72()

def main():
    prefix = os.environ.get("MYPVPREFIX")
    loop = asyncio.new_event_loop()
    RunStarter(prefix, loop)


if __name__ == "__main__":
    main()
