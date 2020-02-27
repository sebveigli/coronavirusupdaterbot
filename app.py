import utils.application

from client.discord_client import DiscordClient
from gateways.bno_news_gateway import BnoNewsGateway
from services.data_parser_service import DataParserService
from services.updater_service import UpdaterService

if __name__ == "__main__":
    args = utils.application.parse_args()

    utils.application.init_logger(args.severity)

    updater_service = UpdaterService(
        BnoNewsGateway(), DataParserService(), args.region, args.frequency, args.channel, args.output,
    )

    discord = DiscordClient(updater_service)
    discord.run(args.token)
