# Pubmed Scraping

This is an implementation of an Asynchronous Pubmed Scraper. 
It allows for controlled concurrency and saves files in `.parquet` format, optimizing data storage efficiency.


## Output File Columns

| Column Name      | Description                                       |
|------------------|---------------------------------------------------|
| url              | The URL of the source document.                   |
| pmid             | The PubMed ID of the document.                   |
| abstract         | The abstract or summary of the document.         |
| keywords         | Keywords associated with the document.            |
| published_date   | The publication date of the document.            |
| citation_doi     | The DOI (Digital Object Identifier) of the document citation. |
| journal          | The journal where the document was published.    |
| volume           | The volume of the journal where the document was published. |
| issue            | The issue number of the journal.                 |
| pages            | The page numbers of the document in the journal. |


## Table example

| url             | pmid      | abstract                                      | keywords        | published_date | citation_doi       | journal            | volume | issue | pages |
|-----------------|-----------|----------------------------------------------|-----------------|----------------|--------------------|--------------------|--------|-------|-------|
| Example URL     | 123456    | This is an example abstract for documentation purposes. | keyword1, keyword2 | 2023-09-21     | 10.12345/example   | Example Journal    | 42     | 3     | 101-120 |

## Clone project


```shell
git clone https://github.com/FourierMourier/async_pubmed_scraper.git
```

## Setup environment

### Using conda

#### Create env
```shell
conda create --name {env} python=3.10
```

#### Activate env
```shell
conda activate {env}
```

### Using venv

#### Create env
```shell
python -m venv venv
```

#### Activate env

windows
```shell
venv\Scripts\activate.bat
```
posix
```shell
source venv/bin/activate
```

### Install requirements
```shell
cd async_pubmed_scraper
pip install -r "requirements.txt"
```
There you go

## User agents list
**Scraping User Agents List:**
Before starting the scraping process, you'll need a list of user agents. You can download the user agents list,
for example, [here](https://seolik.ru/user-agents-list) or anywhere else you'd like.


## Run
To use it, please copy the example config file main.example.yaml to main.yaml, and adjust your paths:
```yaml
user_agents_list_path: path/to/your/agents_list.txt
# sets the directory path where you'll get the results from pubmed
output_dir: collected_data
```
Then, you can run `main.py`.

## Some implementation notes

**Optimal Implementation Approach:**
When it comes to implementation, you might be tempted to use a straightforward approach where you create 
an `asyncio.Task` for each individual task, such as checking URLs, and call these tasks as soon as you reach them. 
For instance, when processing a single page, you might create multiple tasks to process individual URLs representing 
article pages. However, using this approach, you'll be sharing the same `aiohttp.ClientSession` across all tasks, 
and any server error could potentially disrupt the page-wise pipeline:

![Image](assets/async_scraping_v2.png)

**Alternative Approach for Enhanced Clarity:**
Alternatively, you can wait for all tasks corresponding to a specific real task (e.g., collecting all pages to process) 
before moving on to the next step (collecting all URLs to be parsed). In this scenario, the client session object will 
be associated with the same group of tasks and won't be nested within other tasks, leading to a clearer process flow.

By the way, 
1) without the control of concurrency using asyncio.Semaphore it's too likely for you to get banned by pubmed.
2) asyncio.Lock object for the safe session reopening
3) some pages indeed do NOT have abstract content so you might see smth like 
   "...doesn't have abstract-content selected!" in the terminal


# License
This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

# Acknowledgments
This README was created with the assistance of OpenAI's ChatGPT (September 25 Version), a large language model.
You can learn more about it [here](https://chat.openai.com/chat)
