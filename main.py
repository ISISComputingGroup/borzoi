import asyncio
import json
import logging
import os
import uuid

from aioca import caget, camonitor
from aiokafka import AIOKafkaProducer
from epicscorelibs.ca.dbr import DBR_CHAR_BYTES, ca_bytes
from ibex_non_ca_helpers.compress_hex import dehex_decompress_and_dejson
from streaming_data_types.run_start_pl72 import serialise_pl72
from streaming_data_types.run_stop_6s4t import serialise_6s4t

logger = logging.getLogger("borzoi")
logging.basicConfig(level=logging.INFO)


class RunStarter:
    def __init__(
        self, prefix: str, instrument_name: str, producer: AIOKafkaProducer, topic: str
    ) -> None:
        self.producer = None
        self.prefix = prefix
        self.blocks = []
        self.current_job_id = ""
        self.producer = producer
        self.instrument_name = instrument_name
        self.topic = topic
        self.current_run_number = None
        self.current_start_time_ms = None

    async def set_up_monitors(self) -> None:
        logger.info("Setting up monitors")
        camonitor(
            f"{self.prefix}CS:BLOCKSERVER:BLOCKNAMES",
            callback=self._update_blocks,
            datatype=DBR_CHAR_BYTES,
        )
        camonitor(
            f"{self.prefix}DAE:RUNSTATE",
            callback=self._react_to_runstate_change,
            all_updates=True,
            datatype=str,
        )
        camonitor(
            f"{self.prefix}DAE:RUNNUMBER",
            callback=self._update_run_number,
            all_updates=True,
            datatype=int,
        )
        camonitor(
            f"{self.prefix}DAE:START_TIME",
            callback=self._update_start_time_ms,
            all_updates=True,
            datatype=int,
        )

    def _update_run_number(self, value: int) -> None:
        # Cache this as we want the run start message construction and production to be as fast as
        # possible so we don't miss events
        logger.debug(f"Run number updated to {value}")
        self.current_run_number = value

    def _update_start_time_ms(self, value: int) -> None:
        # Cache this as we want the run start message construction and production to be as fast as
        # possible so we don't miss events
        logger.debug(f"Run start time updated to {value} so changing it to ms ({value * 1000})")
        self.current_start_time_ms = value * 1000

    def _update_blocks(self, value: ca_bytes) -> None:
        logger.debug(f"blocks_hexed: {value}")
        blocks_unhexed = dehex_decompress_and_dejson(bytes(value))
        logger.debug(f"blocks_unhexed: {blocks_unhexed}")
        self.blocks = [f"{self.prefix}CS:SB:{x}" for x in blocks_unhexed]

    async def _react_to_runstate_change(self, value: str) -> None:
        logger.info(f"Runstate changed to {value}")

        if value == "BEGINNING":
            self.current_job_id = str(uuid.uuid4())
            await self.construct_and_send_runstart(self.current_job_id)
        elif value == "ENDING":
            await self.construct_and_send_runstop(self.current_job_id)

    async def construct_and_send_runstart(self, job_id: str) -> None:
        logger.info(f"Sending run start with job_id: {job_id}")
        start_time_ms = self.current_start_time_ms
        logger.info(f"Start time: {start_time_ms}")

        runnum = self.current_run_number

        nexus_structure = {
            "children": [
                {
                    "type": "group",
                    "name": "raw_data_1",
                    "children": [
                        {
                            "type": "group",
                            "name": "events",
                            "children": [
                                {
                                    "type": "stream",
                                    "stream": {
                                        "topic": f"{self.instrument_name}_events",
                                        "source": "ISISICP",
                                        "writer_module": "ev42",
                                    },
                                },
                            ],
                            "attributes": [{"name": "NX_class", "values": "NXentry"}],
                        },
                        {
                            "type": "group",
                            "name": "selog",
                            "children": [
                                {
                                    "type": "stream",
                                    "stream": {
                                        "topic": f"{self.instrument_name}_sampleEnv",
                                        "source": x,
                                        "writer_module": "f144",
                                    },
                                }
                                for x in self.blocks
                            ],
                        },
                    ],
                    "attributes": [{"name": "NX_class", "values": "NXentry"}],
                }
            ]
        }
        filename = f"{self.instrument_name}{runnum}.nxs"

        blob = serialise_pl72(
            job_id,
            filename=filename,
            start_time=start_time_ms,
            nexus_structure=json.dumps(nexus_structure),
            run_name=runnum,
        )
        await self.producer.send(self.topic, blob)

    async def construct_and_send_runstop(self, job_id: str) -> None:
        logger.info(f"Sending run stop with job_id: {job_id}")
        # stop_time only gets set to a non-zero value when the runstate goes back to SETUP.
        # This is dirty, but poll it every 0.5 seconds until it does.
        while (
            current_runstate := await caget(f"{self.prefix}DAE:RUNSTATE", datatype=str) != "SETUP"
        ):
            logger.debug(f"Waiting for run state to go back to SETUP. Currently {current_runstate}")
            await asyncio.sleep(0.5)

        stop_time_s = await caget(f"{self.prefix}DAE:STOP_TIME")
        if stop_time_s is None:
            logger.error(f"Failed to get stop time from {job_id}")
            return
        stop_time_ms = int(stop_time_s * 1000)
        logger.info(f"stop time: {stop_time_ms}")
        blob = serialise_6s4t(job_id, stop_time=stop_time_ms, command_id=self.current_job_id)
        await self.producer.send(self.topic, blob)


async def set_up_producer(broker: str) -> AIOKafkaProducer:
    producer = AIOKafkaProducer(bootstrap_servers=broker)
    await producer.start()
    return producer


def main() -> None:
    prefix = os.environ.get("MYPVPREFIX")
    instrument_name = os.environ.get("INSTRUMENT")

    if prefix is None or instrument_name is None:
        raise ValueError("prefix or instrument name not set - have you run config_env.bat?")

    broker = os.environ.get("BORZOI_KAFKA_BROKER", "livedata.isis.cclrc.ac.uk:31092")
    topic = os.environ.get("BORZOI_TOPIC", f"{instrument_name}_runInfo")
    logger.info("setting up producer")
    loop = asyncio.new_event_loop()
    producer = loop.run_until_complete(set_up_producer(broker))
    logger.info("set up producer")

    logger.info("starting run starter")
    run_starter = RunStarter(prefix, instrument_name, producer, topic)
    loop.create_task(run_starter.set_up_monitors())
    loop.run_forever()


if __name__ == "__main__":
    main()
