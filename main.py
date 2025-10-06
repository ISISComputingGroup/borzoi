import asyncio
import json
import logging
import os
import uuid

from aioca import camonitor
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
        self.current_start_time = None
        self.current_stop_time = None

    async def set_up_monitors(self) -> None:
        logger.info("Setting up monitors")
        camonitor(
            f"{self.prefix}CS:BLOCKSERVER:BLOCKNAMES",
            callback=self._update_blocks,
            all_updates=True,
            datatype=DBR_CHAR_BYTES,
        )
        camonitor(
            f"{self.prefix}DAE:RUNNUMBER",
            callback=self._update_run_number,
            all_updates=True,
            datatype=str,
        )
        camonitor(
            f"{self.prefix}DAE:START_TIME",
            callback=self.construct_and_send_runstart,
            all_updates=True,
            datatype=float,
        )
        camonitor(
            f"{self.prefix}DAE:STOP_TIME",
            self.construct_and_send_runstop,
            all_updates=True,
            datatype=float,
        )

    def _update_run_number(self, value: int) -> None:
        # Cache this as we want the run start message construction and production to be as fast as
        # possible so we don't miss events
        logger.info(f"Run number updated to {value}")
        self.current_run_number = value

    def _update_blocks(self, value: ca_bytes) -> None:
        logger.debug(f"blocks_hexed: {value}")
        blocks_unhexed = dehex_decompress_and_dejson(bytes(value))
        logger.debug(f"blocks_unhexed: {blocks_unhexed}")
        self.blocks = [f"{self.prefix}CS:SB:{x}" for x in blocks_unhexed]

    async def construct_and_send_runstart(self, value: float | None) -> None:
        if self.current_start_time is None:
            logger.info("Initial update for start time - not sending run start")
            self.current_start_time = value
            return

        if value == self.current_start_time or value is None:
            logger.error("run start time is the same as cached or invalid. ignoring update")
            return

        self.current_start_time = value
        self.current_job_id = str(uuid.uuid4())
        logger.info(f"Sending run start with job_id: {self.current_job_id}")
        start_time_ms = int(self.current_start_time) * 1000
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
            self.current_job_id,
            filename=filename,
            start_time=start_time_ms,
            nexus_structure=json.dumps(nexus_structure),
            run_name=runnum,
            instrument_name=self.instrument_name,
        )
        await self.producer.send(self.topic, blob)
        logger.info(f"Sent {blob} blob")

    async def construct_and_send_runstop(self, value: float) -> None:
        if self.current_stop_time is None:
            self.current_stop_time = value
            logger.info("Initial update for stop time - not sending run stop")
            return

        if value == self.current_stop_time or value is None:
            logger.error("run stop time is the same as cached or invalid")
            return

        if value == 0:
            # This happens when a new run starts
            logger.debug("stop time set to 0")
            self.current_stop_time = value
            return

        self.current_stop_time = value
        logger.info(f"Sending run stop with job_id: {self.current_job_id}")

        stop_time_ms = int(value * 1000)
        logger.info(f"stop time: {stop_time_ms}")
        blob = serialise_6s4t(
            self.current_job_id, stop_time=stop_time_ms, command_id=self.current_job_id
        )
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
