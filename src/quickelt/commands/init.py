import shutil
from pathlib import Path
from typing import Optional
from typing_extensions import Annotated
import typer
import questionary
from rich.console import Console
from rich.panel import Panel

from quickelt.generator import generate_project

console = Console()

BANNER_LINES = [
    "[#ff3333] ██████╗ ██╗   ██╗██╗ ██████╗██╗  ██╗███████╗██╗  ████████╗[/#ff3333]",
    "[#ff9933]██╔═══██╗██║   ██║██║██╔════╝██║ ██╔╝██╔════╝██║  ╚══██╔══╝[/#ff9933]",
    "[#ffff33]██║   ██║██║   ██║██║██║     █████╔╝ █████╗  ██║     ██║   [/#ffff33]",
    "[#33ff33]██║▄▄ ██║██║   ██║██║██║     ██╔═██╗ ██╔══╝  ██║     ██║   [/#33ff33]",
    "[#3399ff]╚██████╔╝╚██████╔╝██║╚██████╗██║  ██╗███████╗███████╗██║   [/#3399ff]",
    "[#9933ff] ╚══▀▀═╝  ╚═════╝ ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚══════╝╚═╝[/#9933ff]"
]

BANNER = "\n".join(BANNER_LINES) + "\n[bold white]Scaffold Production-Ready Lakehouses in seconds[/bold white]"

def init_command(
    project_name: Annotated[Optional[str], typer.Argument(help="Name of the project directory")] = None,
    storage: Annotated[Optional[str], typer.Option("--storage", "-s", help="Cloud / Storage Target (S3, ADLS, GCS, MinIO)")] = None,
    engine: Annotated[Optional[str], typer.Option("--engine", "-e", help="Execution Engine (Polars, DuckDB)")] = None,
    quality: Annotated[Optional[str], typer.Option("--quality", "-q", help="Data Quality (Pandera, Great Expectations, None)")] = None,
    iac: Annotated[Optional[str], typer.Option("--iac", "-i", help="Infrastructure (Terraform, None)")] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation and use defaults when something is omitted")] = False,
):
    """
    Inicializa um novo projeto Lakehouse.
    """
    console.print(Panel.fit(BANNER, border_style="cyan"))
    print()

    STORAGE_MAP = {
        "s3": "AWS S3",
        "aws s3": "AWS S3",
        "adls": "Azure ADLS",
        "azure adls": "Azure ADLS",
        "gcs": "GCP GCS",
        "gcp gcs": "GCP GCS",
        "minio": "Local MinIO",
        "local minio": "Local MinIO"
    }
    
    ENGINE_MAP = {
        "polars": "Polars",
        "duckdb": "DuckDB"
    }
    
    QUALITY_MAP = {
        "pandera + soda core": "Pandera + Soda Core",
        "pandera": "Pandera + Soda Core",
        "great expectations": "Great Expectations",
        "gx": "Great Expectations",
        "none": "None"
    }
    
    IAC_MAP = {
        "terraform": "Terraform (Azure / AWS / GCP)",
        "none": "None"
    }

    all_flags_passed = all(x is not None for x in [project_name, storage, engine, quality, iac])
    is_non_interactive = all_flags_passed or yes

    if is_non_interactive:
        p_name = project_name or "my_lakehouse"
        
        p_storage = storage.lower() if storage else "minio"
        if p_storage not in STORAGE_MAP:
            raise typer.BadParameter(f"Invalid storage: '{storage}'. Valid options are: S3, ADLS, GCS, MinIO")
        final_storage = STORAGE_MAP[p_storage]
        
        p_engine = engine.lower() if engine else "duckdb"
        if p_engine not in ENGINE_MAP:
            raise typer.BadParameter(f"Invalid engine: '{engine}'. Valid options are: Polars, DuckDB")
        final_engine = ENGINE_MAP[p_engine]
        
        p_quality = quality.lower() if quality else "pandera"
        if p_quality not in QUALITY_MAP:
            raise typer.BadParameter(f"Invalid quality: '{quality}'. Valid options are: Pandera, Great Expectations, None")
        final_quality = QUALITY_MAP[p_quality]
        
        p_iac = iac.lower() if iac else "terraform"
        if p_iac not in IAC_MAP:
            raise typer.BadParameter(f"Invalid iac: '{iac}'. Valid options are: Terraform, None")
        final_iac = IAC_MAP[p_iac]
        
    else:
        if not project_name:
            project_name = questionary.text("Project name:", default="my_lakehouse").ask()
            if not project_name:
                console.print("[red]✗ Cancelado pelo usuário.[/red]")
                raise typer.Exit(1)
        
        p_name = project_name

        final_storage = questionary.select(
            "Select Cloud / Storage Target:",
            choices=["AWS S3", "Azure ADLS", "GCP GCS", "Local MinIO"]
        ).ask()
        
        final_engine = questionary.select(
            "Execution Engine:",
            choices=["Polars", "DuckDB"]
        ).ask()
        
        final_quality = questionary.select(
            "Data Quality & Contracts:",
            choices=["Pandera + Soda Core", "Great Expectations", "None"]
        ).ask()
        
        final_iac = questionary.select(
            "Infrastructure Provisioning:",
            choices=["Terraform (Azure / AWS / GCP)", "None"]
        ).ask()
        
        if not all([final_storage, final_engine, final_quality, final_iac]):
            console.print("[red]✗ Cancelado pelo usuário.[/red]")
            raise typer.Exit(1)

    project_path = Path.cwd() / p_name

    if project_path.exists():
        console.print(f"[yellow]⚠️ The directory './{p_name}' already exists.[/yellow]")
        if yes:
            overwrite = True
        else:
            overwrite = questionary.confirm("Do you want to overwrite it? (This will delete existing contents)").ask()
            
        if not overwrite:
            console.print("[red]✗ Aborted.[/red]")
            raise typer.Exit(1)
        shutil.rmtree(project_path)

    console.print(f"\n[bold]🛠️ Creating Lakehouse architecture in: [cyan]./{p_name}/[/cyan][/bold]\n")
    
    params = {
        "cloud_target": final_storage,
        "engine": final_engine,
        "data_quality": final_quality,
        "infra": final_iac,
        "project_name": p_name
    }
    
    try:
        generate_project(project_path, params)
    except Exception as e:
        console.print(f"[red]✗ Error generating project: {e}[/red]")
        raise typer.Exit(1)

    console.print(f"[bold green]✨ Lakehouse project created successfully![/bold green]\n")
    
    console.print(f"1. cd {p_name}")
    console.print("2. Fill in bucket names & credentials in .env")
    console.print("3. (Optional) Run local MinIO stack: quickelt dev up")
    console.print("4. Run contracts & tests: quickelt dev test")
    console.print("5. Run example pipeline: uv run python -m pipelines.bronze.ingest_example")

