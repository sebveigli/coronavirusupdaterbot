import logging

from argparse import ArgumentParser


def parse_args():
    parser = ArgumentParser()

    parser.add_argument(
        "-t",
        "--token",
        "-d",
        "--discord",
        required=True,
        help="Your Discord bot token, found at https://discordapp.com/developers/applications/",
    )

    parser.add_argument(
        "-c",
        "--channel",
        required=True,
        type=int,
        help="The channel ID where updates should be made (right click on channel in Discord -> Copy ID)",
    )

    parser.add_argument(
        "-f",
        "--frequency",
        required=False,
        default=300,
        type=int,
        help="Data check, refresh and update frequency (in seconds)",
    )

    parser.add_argument(
        "-r",
        "--region",
        required=False,
        default="all",
        choices=["all", "china", "international"],
        help="Choose which region should be checked, refreshed and updated in Discord",
    )

    parser.add_argument(
        "-s",
        "--severity",
        required=False,
        default="info",
        choices=["debug", "info", "warning", "error", "critical"],
        help="Describe the level of logging that should be outputted to the stdout",
    )

    parser.add_argument(
        "-o",
        "--output",
        required=False,
        default="table",
        choices=["table", "text"],
        help="How the updates should be sent to Discord Channels (in table format, or free text sentences)",
    )

    return parser.parse_args()


def init_logger(severity):
    SEVERITY_MAPPER = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    console_logger = logging.StreamHandler()

    console_logging_format = logging.Formatter("%(asctime)s %(levelname)8s -- %(message)s", datefmt="%d/%m %H:%M:%S")
    console_logger.setFormatter(console_logging_format)
    console_logger.setLevel(SEVERITY_MAPPER[severity])

    logger.addHandler(console_logger)

    logger.info(f"{'-' * 60}")
    logger.info("Coronavirus Discord Bot Initiated")
    logger.info("Created by Sebastian Veigli (https://github.com/sebveigli)")
    logger.info(f"{'-' * 60}")
