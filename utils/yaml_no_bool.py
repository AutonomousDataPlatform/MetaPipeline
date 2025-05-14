# utils/yaml_no_bool.py
import yaml
from pathlib import Path
from typing import Union, IO, Any

class NoBoolLoader(yaml.SafeLoader):
    """'on/off/yes/no'를 bool로 변환하지 않는 Loader"""
    pass

# bool 관련 implicit resolver 모두 제거
for key, resolvers in list(NoBoolLoader.yaml_implicit_resolvers.items()):
    NoBoolLoader.yaml_implicit_resolvers[key] = [
        (tag, regexp) for tag, regexp in resolvers
        if tag != 'tag:yaml.org,2002:bool'
    ]

def load_yaml(src: Union[str, Path, IO[str]]) -> Any:
    """
    YAML을 파싱하되 'yes', 'no', 'on', 'off' 등은 문자열로 처리
    """
    if hasattr(src, "read"):
        return yaml.load(src, Loader=NoBoolLoader)
    with open(src, encoding="utf-8") as f:
        return yaml.load(f, Loader=NoBoolLoader)
