import requests
from bs4 import BeautifulSoup

# Some of these sites redirect-loop or block default python-requests UAs.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

# A 200 that returns a shell page or an error template is a failed fetch as far
# as we're concerned -- see FetchError handling in enrich_tv.py.
MIN_BODY_CHARS = 500


class FetchError(RuntimeError):
    """Source could not be fetched or came back unusably thin."""


def html_to_text(html: str, max_chars: int) -> str:
    """
    Reduce a page to its visible text.

    Deliberately generic: no CSS selectors, no per-site knowledge. Everything
    that understands the page's structure is the model's job -- this only exists
    so we send ~10k tokens of text instead of ~150k tokens of markup.
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "iframe"]):
        tag.decompose()

    text = soup.get_text(" ", strip=True)
    text = " ".join(text.split())
    return text[:max_chars]


def fetch_text(url: str, max_chars: int, timeout: int = 30) -> str:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html"},
            timeout=timeout,
        )
    except requests.TooManyRedirects as exc:
        raise FetchError(f"{url} -> redirect loop ({exc})") from exc
    except requests.RequestException as exc:
        raise FetchError(f"{url} -> request failed ({exc})") from exc

    if resp.status_code != 200:
        raise FetchError(f"{url} -> HTTP {resp.status_code}")

    if len(resp.text) < MIN_BODY_CHARS:
        raise FetchError(
            f"{url} -> body only {len(resp.text)} chars, below {MIN_BODY_CHARS} floor"
        )

    text = html_to_text(resp.text, max_chars)
    if len(text) < MIN_BODY_CHARS:
        raise FetchError(
            f"{url} -> extracted only {len(text)} chars of visible text; "
            "page is probably JS-rendered or an error template"
        )

    return text
