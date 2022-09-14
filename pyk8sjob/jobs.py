import inspect
import itertools
import json
import re
import textwrap
from dataclasses import dataclass
from typing import Callable, Optional, Dict

from pyk8sjob.client import K8sJobSpec


@dataclass
class PythonFuncK8sJob(K8sJobSpec):
    """
    Takes a self-contained python function and converts it to a job command.

    simplified version of
    https://github.com/kubeflow/pipelines/blob/0.1.8/sdk/python/kfp/components/_python_op.py#L511
    """

    func: Optional[Callable] = None
    kwargs: Optional[Dict] = None

    def __post_init__(self):
        self.full_source = self._create_source(self.func, self.kwargs)

    def _parse_arg(self, arg):
        if isinstance(arg, int):
            return arg
        elif isinstance(arg, bool):
            return arg
        else:
            return json.dumps(arg)

    def parse_kwargs(self, kwargs):
        kwargs_str = ""
        if kwargs is not None:
            kwargs_str = []
            for k, v in kwargs.items():
                kwargs_str.append(f"{k}={self._parse_arg(v)}")
            kwargs_str = ",".join(kwargs_str)
        return kwargs_str

    def _create_source(self, func: Callable, kwargs: Optional[Dict] = None):
        func_code = self._get_function_source_definition(func)
        kwargs_str = ""
        if kwargs is not None:
            kwargs_str = []
            for k, v in kwargs.items():
                kwargs_str.append(f"{k}={self._parse_arg(v)}")
            kwargs_str = ",".join(kwargs_str)

        full_source = f"""\
{func_code} 
_outputs = {func.__name__}({kwargs_str})
"""
        return re.sub("\n\n\n+", "\n\n", full_source).strip("\n") + "\n"

    @staticmethod
    def _get_function_source_definition(func: Callable) -> str:
        func_code = inspect.getsource(func)

        # Function might be defined in some indented scope (e.g. in another
        # function). We need to handle this and properly dedent the function source
        # code
        func_code = textwrap.dedent(func_code)
        func_code_lines = func_code.split("\n")

        # Removing possible decorators (can be multiline) until the function
        # definition is found
        func_code_lines = itertools.dropwhile(
            lambda x: not x.startswith("def"), func_code_lines
        )

        if not func_code_lines:
            raise ValueError(
                'Failed to dedent and clean up the source of function "{}". '
                "It is probably not properly indented.".format(func.__name__)
            )

        return "\n".join(func_code_lines)

    @property
    def command(self):
        return [
            "sh",
            "-ec",
            textwrap.dedent(
                """\
                    program_path=$(mktemp)
                    printf "%s" "$0" > "$program_path"
                    python3 -u "$program_path" "$@"
                """
            ),
            self.full_source,
        ]


@dataclass
class PythonScriptK8sJob(K8sJobSpec):
    """
    Takes a self-contained python function and converts it to a job command.

    simplified version of
    https://github.com/kubeflow/pipelines/blob/0.1.8/sdk/python/kfp/components/_python_op.py#L511
    """

    path: Optional[str] = None

    def __post_init__(self):
        self.full_source = self._create_source(self.path)

    def _create_source(self, path: str):
        with open(path, "r") as f:
            contents = f.readlines()
        full_source = "".join(contents)
        return re.sub("\n\n\n+", "\n\n", full_source).strip("\n") + "\n"

    @property
    def command(self):
        return [
            "sh",
            "-ec",
            textwrap.dedent(
                """\
                    program_path=$(mktemp)
                    printf "%s" "$0" > "$program_path"
                    python3 -u "$program_path" "$@"
                """
            ),
            self.full_source,
        ]
