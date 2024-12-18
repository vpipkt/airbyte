from textwrap import dedent

from typing import Union
import logging

def get_dict_texts(d: dict, keys: list[str]) -> list[str]:
    result = []
    for k, v in d.items():
        if isinstance(v, str) and k in keys:
            result.append(v)
        if isinstance(v, dict):
            result.extend(get_dict_texts(v, keys))
        if isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    result.extend(get_dict_texts(item, keys))

    return result

def say_dicts(l: list[dict], keys: list[str]) -> str:
    result: list[str] = []
    for d in l:
        result.extend(get_dict_texts(d, keys))

    concat = " ".join(result)
    return say(concat)


def say(args: Union[str, None]) -> str:

    if not args:
        return ""
    topbar = "-" * len(args)
    bottombar = "-" * len(args)
    output = dedent(
        """
      %s
    < %s >
      %s
       \   ^__^
        \  (oo)\_______
           (__)\       )\/\\
               ||----w |
               ||     ||
    """
        % (topbar, args, bottombar)
    )
    return(output)
