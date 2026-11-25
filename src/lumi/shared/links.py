import re


def source_urls(content: str) -> frozenset[str]:

    result = set()
    for url in re.findall(r"https://[^\s<>]+", content):
        url = url.rstrip(".,;:!?")
        for opening, closing in (("(", ")"), ("[", "]"), ("{", "}")):
            while url.endswith(closing) and url.count(closing) > url.count(opening):
                url = url[:-1]
        result.add(url)
    return frozenset(result)
