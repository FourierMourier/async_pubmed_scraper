from pathlib import Path
from bs4 import BeautifulSoup
from collections import OrderedDict

from typing import Optional


__all__ = ['process_pubmed_page_text', 'PUBMED_BASE_URL']


PUBMED_BASE_URL: str = "https://pubmed.ncbi.nlm.nih.gov"


def process_pubmed_page_text(text: Optional[str], url: str, verbose: bool = False,
                             error_on_null_id: bool = False) -> OrderedDict:
    """

    :param text:
    :param url:
    :param verbose:
    :param error_on_null_id:
    :return:
    """
    none_respond = OrderedDict(**{
            "url": url,
            "pmid": None,
            "abstract": None,
            "keywords": None,
            "published_date": None,
            "citation_doi": None,
            'journal': None,
            "volume": None,
            "issue": None,
            "pages": None,
        })

    if text is None:
        return none_respond

    article_soup = BeautifulSoup(text, "html.parser")
    if article_soup is None:
        print(f"cannot parse url={url}: text={text}")
        return none_respond
    # Extract the abstract
    abstract_content_class = "abstract-content"
    abstract_content_selected_class ="abstract-content selected"
    # -------------
    abstract_content_element = article_soup.find("div", class_=abstract_content_class)
    abstract = abstract_content_element.text.strip() if abstract_content_element else None  # "No abstract available."
    if abstract is None:
        # raise AssertionError(f"cannot parse abstract: abstract={abstract}")
        print(f"{url} doesn't have {abstract_content_class}! Trying to access {abstract_content_selected_class} class")
        abstract_content_element = article_soup.find("div", class_=abstract_content_selected_class)
        abstract = abstract_content_element.text.strip() if abstract_content_element else None  # "No abstract available."
        if abstract is None:
            print(f"{url} doesn't have {abstract_content_selected_class}!")
            return none_respond
        else:
            print(f"{url} does have an {abstract_content_selected_class}")

    # Find the keywords element and extract its text
    # abstract_element = article_soup.find("div", class_="abstract")
    # keywords_element = article_soup.find("strong", class_="sub-title").find_next_sibling("p")
    # keywords = keywords_element.text.strip() if keywords_element else "No keywords available."
    abstract_content = article_soup.find("div", class_="abstract")
    keywords = None
    if abstract_content:
        # Find the keywords element and extract its text
        keywords_element = abstract_content.find("strong", class_="sub-title")
        if keywords_element and keywords_element.nextSibling:
            keywords = keywords_element.nextSibling.text.strip()

    # keywords = keywords_element.text.strip() if keywords_element else "No keywords available."
    # abstract_text = abstract_content.get_text(strip=True)
    # keywords_start = abstract_text.find("Keywords:") + len("Keywords:")
    # keywords = abstract_text[keywords_start:].strip()
    # Find the PMID element
    pmid_element = article_soup.find("span", class_="identifier pubmed")

    if pmid_element:
        pmid_element = pmid_element.find("strong", class_="current-id")
    else:
        msg = f"url={url}: cannot parse identifier: {pmid_element}"
        if error_on_null_id:
            raise AssertionError(msg)
        else:  # then simply skip since you can't do anything with that
            print(msg)

    pmid = pmid_element.text.strip() if pmid_element else None  # "No PMID available."
    # Extract the topics
    # topics = [topic.text.strip() for topic in article_soup.find_all("a", class_="tag")]
    # Extract the published date
    cit_element = article_soup.find("span", class_="cit")
    cit: Optional[str] = cit_element.text.strip() if cit_element else None
    citation_doi_element = article_soup.find("span", class_="citation-doi")
    citation_doi = citation_doi_element.text.strip() if citation_doi_element else None
    #  "No published date available."

    # Extract the journal
    # journal_element = article_soup.find("a", class_="journal-title-link")
    # journal = journal_element.text.strip() if journal_element else "No journal information available."
    journal_element = article_soup.find("button", class_="journal-actions-trigger")
    journal: Optional[str] = journal_element.text.strip() if journal_element else None

    published_date, volume, issue, pages = None, None, None, None
    if cit:
        # Split the string by semicolon to get the published date and the rest of the citation
        try:
            cit_splitted = cit.split(';')
            published_date = cit_splitted[0]
            rest_of_citation = cit_splitted[1:]
            if len(rest_of_citation) == 1:
                rest_of_citation = rest_of_citation[0]
            else:
                rest_of_citation = ';'.join(rest_of_citation)

            # Split the rest of the citation by colon to get the volume, issue, and pages
            volume_issue, pages = rest_of_citation.split(':')

            # Split the volume and issue by parentheses to get the volume and issue numbers
            vi_splitted = volume_issue.split('(')
            if len(vi_splitted) == 2:
                volume = vi_splitted[0]
                issue = vi_splitted[1][:-1]
            elif len(vi_splitted) == 1:
                if verbose:
                    print(f"volume_issue.split('(')={vi_splitted}!!!")
                volume = vi_splitted[0]
                issue = None
            else:
                print(f"volume_issue.split('(')={vi_splitted}!!!")
        except Exception as E:
            print(f"url={url}, cit={cit}: {E}")

    return OrderedDict(**{
        "url": url,
        "pmid": pmid,
        "abstract": abstract,
        "keywords": keywords,
        "published_date": published_date,
        "citation_doi": citation_doi,
        'journal': journal,
        "volume": volume,
        "issue": issue,
        "pages": pages,
    })
