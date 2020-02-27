import discord
import logging


class DiscordClient(discord.Client):
    def __init__(self, updater_service, loop=None, **options):
        super().__init__(loop=loop, **options)

        self.updater_service = updater_service
        self.logger = logging.getLogger(__name__)

    async def on_ready(self):
        self.logger.info("Connected to Discord!")
        self.logger.info(f"Logged in as @{self.user.name}#{self.user.discriminator} [ID: {self.user.id}]")

        await self.wait_until_ready()
        await self.change_presence(activity=discord.Game("😷"))

        self.loop.create_task(self.updater_service.update_loop(self))
