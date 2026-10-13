"""Command line entry point.

`serve` is the control plane: the MCP server, the queues, and the job launcher.
`job` is the job-pod entrypoint: one task, one pod.
"""

import argparse
import logging
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

import anyio
import uvicorn

from robo_worker.asker import Asker
from robo_worker.config import Config, ConfigError, load_config
from robo_worker.job_main import job_main
from robo_worker.launcher import K8sLauncher, LauncherError
from robo_worker.scheduler import Scheduler
from robo_worker.secret_files import SecretError, load_callers
from robo_worker.server import build_app
from robo_worker.store import JobStore

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
_CONFIG_HELP = "TOML config file (see config.example.toml)"


async def serve(cfg: Config, callers: Mapping[str, str]) -> None:
    home = os.environ.get("HOME")
    if home:
        # HOME is on the state claim in the cluster, and a new claim is empty.
        Path(home).mkdir(parents=True, exist_ok=True)
    store = JobStore(cfg.state_root)
    launcher = K8sLauncher(cfg, cfg.job_template)
    # A launcher that cannot start pods would fail every queued job, one at a time.
    await launcher.check()
    scheduler = Scheduler(cfg, store, launch=launcher)
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


def _attempt(text: str) -> int:
    if not text.isascii() or not text.isdigit() or int(text) < 1:
        raise argparse.ArgumentTypeError(f"must be a whole number of 1 or more, got {text!r}")
    return int(text)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="robo-worker",
        description="Run claude -p jobs on repo checkouts and serve them over MCP.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    control = sub.add_parser("serve", help="run the control plane (MCP, queues, launcher)")
    control.add_argument("--config", type=Path, required=True, help=_CONFIG_HELP)
    job = sub.add_parser("job", help="run one job in this pod, then exit")
    job.add_argument("--config", type=Path, required=True, help=_CONFIG_HELP)
    job.add_argument("--job-id", required=True, help="the job to run, as submit_job returned it")
    job.add_argument(
        "--attempt", type=_attempt, required=True, help="the attempt number, 1 or more"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)
    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"robo-worker: {exc}", file=sys.stderr)
        return 2
    if args.command == "job":
        return job_main(cfg, args.job_id, args.attempt)
    try:
        callers = load_callers(cfg.callers_file)
    except SecretError as exc:
        print(f"robo-worker: {exc}", file=sys.stderr)
        return 2
    try:
        anyio.run(serve, cfg, callers)
    except LauncherError as exc:
        print(f"robo-worker: cannot start job pods: {exc}", file=sys.stderr)
        return 2
    return 0
