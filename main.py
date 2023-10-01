from pathlib import Path
import random
import time
import sys
import asyncio

from collections import OrderedDict

import aiohttp
import pandas as pd
import pydantic

from bs4 import BeautifulSoup

from scraping.common import *
from scraping.aio import AsyncUserAgentManager
from utils.common import load_yaml

import logging

from typing import Optional, Dict, List, Iterable, Coroutine, Awaitable, Any, Callable, Tuple


def flatten_container(container: Iterable[Any], exclude_none: bool = True) -> List[Any]:
    result = []

    def _flatten(obj):
        if isinstance(obj, (list, tuple, set)):
            for item in obj:
                _flatten(item)
        else:
            if not (exclude_none and obj is None):
                result.append(obj)

    _flatten(container)
    return result


async def async_scrape_article_with_semaphore(
        semaphore: asyncio.Semaphore, client_session: aiohttp.ClientSession, url: str, verbose: bool = False,
        error_on_null_id: bool = True) -> OrderedDict:

    if verbose:
        print(f"waiting semaphore={semaphore} to access {url} ...")
    async with semaphore:
        if verbose:
            print(f"semaphore intercepted, accessing {url} ...")

        async with client_session.get(url) as response:
            text = await response.text()
            if verbose and text is None:
                print(f"text={text} at url={url}")
                raise AssertionError(f"got text={text}")
            return process_pubmed_page_text(text, url, verbose, error_on_null_id)


async def extract_urls_from_page(semaphore: asyncio.Semaphore, client_session: aiohttp.ClientSession, base_url: str,
                                 params, verbose: bool = True, parser: str = "html.parser") -> List[str]:

    if verbose:
        print(f"waiting semaphore={semaphore} to access {base_url}")

    async with semaphore:
        if verbose:
            print(f"semaphore intercepted, accessing {base_url} ...")
        async with client_session.get(base_url, params=params) as response:
            if response.status == 200:
                page_text = await response.text()
                soup = BeautifulSoup(page_text, parser)
                search_results = soup.find_all("div", class_="docsum-content")
                urls = []

                for result in search_results:
                    title_element = result.find("a", class_="docsum-title")
                    url = base_url + title_element["href"]
                    urls.append(url)

                return urls  # identifier, urls
            else:  # raise error so that the loop above will catch it and process again
                raise ConnectionError(f"response={response.status}")


async def process_tasks_with_retry(task_executor: Callable, task_params: Dict[int, Any], max_retries,
                                   session_manager: AsyncUserAgentManager, verbose: bool = True):
    task_map = {}
    errors_map = {}

    for ident, other_params in task_params.items():
        client_session = await session_manager.get_client_session()
        task = asyncio.create_task(task_executor(session_manager.semaphore, client_session, *other_params))
        task.set_name(str(ident))
        task_map[ident] = task
        errors_map[ident] = 0

    pending_tasks = list(task_map.values())
    results = []

    while pending_tasks:  # wait till the first exception and then handle it - it's likely to be the 429 error
        done_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_EXCEPTION)
        for done_task in done_tasks:
            # try to catch the one with exception
            if done_task.exception() is None:
                # task_id, task_result = done_task.result()
                task_result = done_task.result()
                results.append(task_result)
            else:
                # show exception in the console
                task_exc_info = done_task.exception()
                logging.error(f"during processing got exception", exc_info=task_exc_info)
                # find the exact parameters to send task again to be pended
                done_task_name: str = done_task.get_name()
                done_task_name.isdigit(), f"done task wasn't a digit: {done_task_name}"
                identifier = int(done_task_name)
                # ideally it must be in the task_map keys but for operational purposes check it there
                if identifier in task_map:
                    task = task_map[identifier]
                    # cancel task
                    task.cancel()
                    # clean up the memory
                    del task_map[identifier]
                    # find parameters
                    other_params = task_params[identifier]
                    # check for max retries limit:
                    if errors_map[identifier] < max_retries:
                        client_session = await session_manager.get_client_session()
                        new_task = asyncio.create_task(
                            task_executor(session_manager.semaphore, client_session, *other_params))
                        new_task.set_name(str(identifier))
                        task_map[identifier] = new_task
                        errors_map[identifier] += 1
                        if isinstance(pending_tasks, list):
                            pending_tasks.append(new_task)
                        elif isinstance(pending_tasks, set):
                            pending_tasks.add(new_task)
                        else:
                            raise NotImplementedError(f"unknown type for pending_tasks variable={type(pending_tasks)}")
                    else:  # if it exceeds - leave any attempts to access the resource and leave the task "done"
                        if verbose:
                            print((f"leaving attempts to execute {task_executor.__name__} coroutine due to "
                                   f"current retries {errors_map[identifier]} >= {max_retries},"
                                   f"identifiers={identifier}, parameters (except session and semaphore): {other_params}"))
    return results


async def async_search_pubmed(session_manager: AsyncUserAgentManager,
                              query: str,
                              num_pages=2,
                              start_page: int = 1,
                              base_url: Optional[str] = None,
                              verbose: bool = False,
                              max_retries: int = 10,
                              ) -> List[OrderedDict]:
    base_url = base_url or PUBMED_BASE_URL

    term = query.replace(" ", "+")

    page_task_params_map: Dict[int, Any] = {}
    for page in range(start_page, start_page + num_pages + 1):
        params: Dict[str, str] = {'term': term, 'page': str(page)}
        func_params = (base_url, params)  # , verbose)
        page_task_params_map[page] = func_params

    raw_urls_to_process: List[List[str]] = await process_tasks_with_retry(
        extract_urls_from_page, page_task_params_map, max_retries=max_retries,
        session_manager=session_manager)

    urls_to_process = flatten_container(raw_urls_to_process)
    print("*"*50)
    print(f"got {len(urls_to_process)} urls to process for `num_pages`={num_pages}")
    print("*" * 50)
    error_on_null_id: bool = True
    url_task_params_map: Dict[int, Any] = {}
    for url_num, url in enumerate(urls_to_process):
        url_task_params_map[url_num] = (url, verbose, error_on_null_id)

    parsed_url: List[OrderedDict[str, Any]] = await process_tasks_with_retry(
        async_scrape_article_with_semaphore, url_task_params_map, max_retries=max_retries,
        session_manager=session_manager)

    return parsed_url


class StepByStepConfig(pydantic.BaseModel):
    max_agents_num: pydantic.PositiveInt
    max_concurrent_requests: pydantic.PositiveInt
    query: str
    num_pages: pydantic.PositiveInt
    start_page: int
    max_retries: pydantic.PositiveInt
    verbose: bool
    user_agents_list_path: str
    check_interval: pydantic.PositiveFloat
    output_dir: str

    # v1 style:
    # @pydantic.validator('start_page')
    # v2 style:
    @pydantic.field_validator('start_page', mode='after')
    @classmethod
    def validate_start_page(cls, value):
        if value < 1 or value > 1000:
            raise NotImplementedError(f"`start_page` must be in [1, 1000] but got {value}")
        return value


async def main():
    current_file = Path(__file__)
    configs_dir = current_file.parents[0] / 'configs'
    config_filepath = configs_dir / (current_file.stem + '.yaml')
    if config_filepath.exists() is False:
        raise FileNotFoundError(f"not able to find {config_filepath}; try to use "
                                f"{config_filepath.stem + '.example.yaml'} as starting point")

    config = StepByStepConfig(**load_yaml(config_filepath, encoding='utf-8'))

    max_agents_num: int = config.max_agents_num  # 100
    max_concurrent_requests: int = config.max_concurrent_requests  # 40
    query = config.query  # "food allergies"
    num_pages = config.num_pages  # 1000  # 50
    start_page: int = config.start_page  # 1  # first one was 1
    max_retries: int = config.max_retries  # 10
    verbose: bool = config.verbose  # True
    user_agents_list_path: str = config.user_agents_list_path  # ...
    check_interval: float = config.check_interval
    output_dir = config.output_dir

    output_dir = Path(output_dir).absolute()
    # output_dir.parents[0].mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)

    if (start_page + num_pages) > 1001:
        num_pages = max(1000 - start_page, 0)
        print(f"num pages was adjusted to pubmed acceptable {num_pages} value")

    with open(user_agents_list_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        user_agents = [line.strip() for line in lines]

    session_manager = AsyncUserAgentManager(user_agents, max_agents_num=max_agents_num,
                                            max_concurrent_requests=max_concurrent_requests)
    # # uncomment once .check_session_health code is not empty
    # await session_manager.start_monitoring(check_url=PUBMED_BASE_URL, check_interval=check_interval)
    s0 = time.time()
    results = await async_search_pubmed(session_manager, query, num_pages, start_page, verbose=verbose,
                                        max_retries=max_retries)
    await session_manager.shutdown()
    print(f"time needed {time.time() - s0:.3f} sec for num_pages={num_pages}")
    output = pd.DataFrame()
    for url_res in results:
        if url_res is not None:
            output = pd.concat([output, pd.DataFrame([url_res])], axis=0, ignore_index=True)

    save_filepath = output_dir / f"{query.replace(' ', '+')}_pubmed={start_page}_pages={num_pages}.parquet"
    print(f"saving output of shape {output.shape} at path {save_filepath}")
    output.to_parquet(save_filepath)


if __name__ == '__main__':
    if 'win' in sys.platform:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
