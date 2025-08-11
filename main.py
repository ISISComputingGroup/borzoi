import os
import uuid
import logging

from aioca import run, camonitor, caget
from epicscorelibs.ca.dbr import DBR_CHAR_BYTES
from ibex_non_ca_helpers.hex import dehex_decompress_and_dejson

from streaming_data_types.run_start_pl72 import serialise_pl72
from streaming_data_types.run_stop_6s4t import serialise_6s4t

logger = logging.getLogger("blah123")
logging.basicConfig(level=logging.INFO)


class RunStarter:

    # This class needs to
    # have a function which handles
    def __init__(self, prefix, ):
        self.producer = None
        self.prefix = prefix
        self.blocks = []
        self.current_job_id = ""
        camonitor(f"{self.prefix}CS:BLOCKSERVER:BLOCKNAMES", callback=self.update_blocks, datatype=DBR_CHAR_BYTES)
        pass

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
        pass

    async def construct_and_send_runstart(self, job_id: str, prefix):
        logger.info(f"Sending run start with job_id: {job_id}")
        start_time = await caget(f"{prefix}DAE:STARTTIME")
        # TODO get start time as unix timestamp here
        # TODO construct nexus json here


async def set_up_monitor(prefix, runstarter):
    logger.info("setting up monitor with prefix %s", prefix)
    camonitor(f"{prefix}DAE:RUNSTATE", callback=runstarter.react_to_runstate_change, all_updates=True, datatype=str)

def main():
    prefix = os.environ.get("MYPVPREFIX")
    # TODO do i need to set up an event loop here and pass it into the RunStarter instance as well as using it below?
    runstarter = RunStarter(prefix)

    run(set_up_monitor(prefix, runstarter), forever=True)


if __name__ == "__main__":
    main()
