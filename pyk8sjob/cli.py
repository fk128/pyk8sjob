import dataclasses
import os
import uuid
from pathlib import Path
from typing import Optional, List

import typer

from pyk8sjob.client import K8sJobsClient, sanitize_name
from pyk8sjob.config import config
from pyk8sjob.jobs import PythonScriptK8sJob
from pyk8sjob.utils import Preset, get_preset, parse_env

app = typer.Typer()
presets_app = typer.Typer()
app.add_typer(presets_app, name="presets", help="view/edit presets")


@presets_app.command("edit", help="edit presets")
def edit_preset():
    path = Path(config.presets_path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
    if "EDITOR" in os.environ:
        os.system(f"$EDITOR {config.presets_path}")
    else:
        typer.echo(
            "$EDITOR env variable not set. Set it to be able to edit, e.g. export $EDITOR=vim",
            err=True,
        )


@presets_app.command("view", help="view presets")
def view_preset():
    path = Path(config.presets_path)
    if path.exists():
        from rich.console import Console
        from rich.syntax import Syntax

        console = Console()
        with open(config.presets_path, "rt") as code_file:
            syntax = Syntax(code_file.read(), "yaml")
        console.print(syntax)


@presets_app.command("template", help="view preset template")
def view_preset_template():
    from rich.console import Console

    console = Console()
    console.print(Preset())


@app.command("submit", help="Submit script as job to k8s cluster")
def submit(
    path: str,
    namespace: str = "default",
    image: Optional[str] = None,
    env: Optional[List[str]] = None,
    preset: Optional[str] = None,
    dryrun: bool = False,
):
    client = K8sJobsClient(namespace=namespace)
    name = sanitize_name(Path(path).stem)
    name = f"{name}-{str(uuid.uuid4())[:4]}"

    job = PythonScriptK8sJob(name=name, path=path)
    if preset:
        preset: Preset = get_preset(preset)
        for field in dataclasses.fields(preset):
            value = getattr(preset, field.name)
            if value is not None:
                setattr(job, field.name, value)
    if image is not None:
        job.image = image
    if env is not None:
        job.env_vars = parse_env(env)

    client.submit(job_spec=job, dryrun=dryrun)


if __name__ == "__main__":
    app()
