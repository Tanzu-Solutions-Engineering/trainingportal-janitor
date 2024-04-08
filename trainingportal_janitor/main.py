import argparse
import datetime
import logging
import time
from collections import Counter

from kubernetes import config, dynamic
from kubernetes.client import api_client
from kubernetes.dynamic.exceptions import ResourceNotFoundError

from trainingportal_janitor import shutdown

logger = logging.getLogger("trainingportal_janitor")
EXPIRY_ANNOTATION = "janitor/expires"
DATETIME_PATTERNS = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M", "%Y-%m-%d"]


def main(args=None):
    parser = get_cmd_parser()
    args = parser.parse_args(args)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
    )

    config_str = ", ".join(f"{k}={v}" for k, v in sorted(vars(args).items()))
    logger.info(f"TrainingPortal Janitor started with {config_str}")

    if args.dry_run:
        logger.info("**DRY-RUN**: no deletions will be performed!")

    handler = shutdown.GracefulShutdown()
    while True:
        try:
            counter: Counter = Counter()
            client = dynamic.DynamicClient(
                api_client.ApiClient(configuration=get_in_cluster_with_fallback())
            )

            sweep_objects(client, "training.educates.dev/v1beta1", counter, args.dry_run)

            stats = ", ".join([f"{k}={v}" for k, v in counter.items()])
            logger.info(f"Clean up run completed: {stats}")

        except Exception as e:
            logger.exception("Failed while running clean up loop: %s", e)
        if args.once or handler.shutdown_now:
            return
        with handler.safe_exit():
            time.sleep(args.interval)

def sweep_objects(client, tp_api_version, counter, dry_run):
    trainingportal_api = client.resources.get(
        api_version=tp_api_version, kind="TrainingPortal"
    )

    logger.debug("Listing %s training portals" %(tp_api_version))
    trainingportals = trainingportal_api.get()
    for trainingportal in trainingportals.items:
        counter.update(handle_trainingportal_expiry(trainingportal_api,
            trainingportal, dry_run))

def get_in_cluster_with_fallback():
    config_obj = None
    logger.debug("Attempting to load in cluster config")
    try:
        config_obj = config.load_incluster_config()
    except Exception as e:
        logger.debug(f"Couldn't load in cluster config: {e}")
        logger.debug("Attempting to load local client config")
        config_obj = config.load_kube_config()


def parse_expiry(expiry: str) -> datetime.datetime:
    for pattern in DATETIME_PATTERNS:
        try:
            return datetime.datetime.strptime(expiry, pattern).replace(tzinfo=None)
        except ValueError:
            pass
    raise ValueError(
        f'expiry value "{expiry}" does not match format 2022-01-01T13:23:54Z, 2022-01-01T13:23, or 2022-01-01'
    )


def delete(trainingportal_api, resource, dry_run: bool):
    if dry_run:
        logger.info(
            f'**DRY-RUN**: would delete TrainingPortal {resource.metadata.name}'
        )
    else:
        logger.info(
            f'Deleting TrainingPortal {resource.metadata.name}..'
        )
        try:
            trainingportal_api.delete(name = resource.metadata.name)
        except Exception as e:
            logger.error(
                f'Could not delete TrainingPortal {resource.metadata.name}: {e}'
            )


def handle_trainingportal_expiry(trainingportal_api, trainingportal, dry_run: bool):
    counter = {}
    expiry = trainingportal.metadata.annotations.get(EXPIRY_ANNOTATION) if trainingportal.metadata.annotations else None
    if expiry:
        reason = f"annotation {EXPIRY_ANNOTATION} is set"
        try:
            expiry_timestamp = parse_expiry(expiry)
        except ValueError as e:
            logger.info(
                f'Ignoring invalid expiry date on TrainingPortal {trainingportal.name}: {e}'
            )
        else:
            counter[f"trainingportal-with-expiry"] = 1
            now = datetime.datetime.utcnow()
            if now > expiry_timestamp:
                message = f'TrainingPortal {trainingportal.metadata.name} expired on {expiry} and will be deleted ({reason})'
                logger.info(message)
                delete(trainingportal_api, trainingportal, dry_run=dry_run)
                counter[f"trainingportal-deleted"] = 1
            else:
                logging.debug(
                    f'TrainingPortal {trainingportal.metadata.name} will expire on {expiry}'
                )
    return counter


def get_cmd_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        help="Dry run mode: do everything but actually delete",
        action="store_true",
    )
    parser.add_argument(
        "--verbose", "-v", help="Verbose logging", action="store_true"
    )
    parser.add_argument(
        "--once", help="Run loop only once and exit", action="store_true"
    )
    parser.add_argument(
        "--interval", type=int, help="Loop interval (default: 30s)", default=30
    )

    return parser
