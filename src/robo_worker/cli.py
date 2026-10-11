"""Command line entry point: robo-worker --config <file>."""

import argparse
import logging
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

import anyio
import uvicorn

from robo_worker.asker import Asker
from robo_worker.config import Config, ConfigError, load_config
from robo_worker.scheduler import Scheduler
from robo_worker.secret_files import SecretError, load_callers
from robo_worker.server import build_app
from robo_worker.store import JobStore


async def serve(cfg: Config, callers: Mapping[str, str]) -> None:
    store = JobStore(cfg.state_root)
    scheduler = Scheduler(cfg, store)
    scheduler.recover()
    asker = Asker(cfg, store, scheduler.pauses)
    app = build_app(cfg, scheduler, asker, callers)
    server = uvicorn.Server(
        uvicorn.Config(app, host=cfg.listen_host, port=cfg.listen_port, lifespan="on")
    )
    async with anyio.create_task_group() as tg:
        tg.start_soon(scheduler.run_forever)
        await server.serve()
        tg.cancel_scope.cancel()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="robo-worker",
        description="Run claude -p jobs on repo checkouts and serve them over MCP.",
    )
    parser.add_argument(
        "--config", type=Path, required=True, help="TOML config file (see config.example.toml)"
    )
    args = parser.parse_args(argv)
    log_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)
    try:
        cfg = load_config(args.config)
        callers = load_callers(cfg.callers_file)
    except (ConfigError, SecretError) as exc:
        print(f"robo-worker: {exc}", file=sys.stderr)
        return 2
    anyio.run(serve, cfg, callers)
    return 0
