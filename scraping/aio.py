import asyncio
import random
from collections import OrderedDict
import aiohttp

from typing import Tuple, List, Union, Iterable, Optional, Any


__all__ = ['AsyncUserAgentManager']


class AsyncUserAgentManager:
    """
        Provides class responsible for session retrieval
            as well as their "health" check - if session is not responding it will be reopened
    """

    USER_AGENT_KEY: str = 'User-Agent'

    def __init__(self, user_agents: Iterable[str], max_agents_num: int = 100,
                 max_concurrent_requests: int = 30,
                 do_shutdown: bool = False):
        self._user_agents = random.choices(tuple(user_agents), k=max_agents_num)
        self.agent_index = 0
        self.client_sessions: OrderedDict[str, aiohttp.ClientSession] = OrderedDict()
        self.lock = asyncio.Lock()
        self.do_shutdown = do_shutdown
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    @property
    def user_agents(self) -> Tuple[str, ...]:
        return self._user_agents

    @user_agents.setter
    def user_agents(self, value: Any):
        raise AssertionError(f"Not allowed to change user agents")

    async def shutdown(self) -> None:
        """
        Shutdowns the monitoring and closes all sessions
        :return:
        """
        self.do_shutdown = True
        await asyncio.sleep(0.01)
        await self.clean_up()

    async def get_client_session(self, return_key: bool = False) -> \
            Union[aiohttp.ClientSession, Tuple[aiohttp.ClientSession, str]]:
        """

        :param return_key:
        :return:
        """
        user_agent = self.user_agents[self.agent_index]
        # ---------------------------------------------
        async with self.semaphore:  # Acquire the semaphore before creating a session
            if user_agent not in self.client_sessions:
                headers = {self.USER_AGENT_KEY: user_agent}
                # extend headers with some additional info
                #   https://stackoverflow.com/a/74674276
                headers['Accept-Language'] = 'en-US,en;q=0.5'
                #
                self.client_sessions[user_agent] = aiohttp.ClientSession(headers=headers)

            # update agent_index
            self._switch_user_agent()
            if not return_key:
                return self.client_sessions[user_agent]
            else:
                return self.client_sessions[user_agent], user_agent

    async def reopen_session(self, key: str) -> bool:
        async with self.lock:
            session: aiohttp.ClientSession = self.client_sessions.get(key, None)
            if session is None:
                return False
            headers = {self.USER_AGENT_KEY: key}
            await session.close()
            # del self.client_sessions[key]
            self.client_sessions[key] = aiohttp.ClientSession(headers=headers)
            return True

    async def close_sessions(self) -> None:
        for session in self.client_sessions.values():
            await session.close()

    def _switch_user_agent(self):
        self.agent_index = (self.agent_index + 1) % len(self.user_agents)

    async def clean_up(self):
        await self.close_sessions()

    async def monitor_sessions(self, check_url: str, check_interval: float = 2.0):
        while True:
            if self.do_shutdown:
                break
            await asyncio.sleep(check_interval)
            await self.check_session_health(check_url)

    async def check_session_health(self, url_to_scrape: str) -> Exception:
        raise NotImplementedError

    async def start_monitoring(self, check_url: str, check_interval: float = 2.0):
        asyncio.create_task(self.monitor_sessions(check_url, check_interval))
