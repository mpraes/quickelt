import subprocess
from rich.console import Console
from rich.panel import Panel

console = Console()

def check_command(cmd: list[str]) -> bool:
    try:
        subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def doctor_command():
    """
    Verifica a presença de ferramentas locais no sistema (python, uv, docker, git).
    """
    console.print(Panel.fit("[bold blue]QuickELT Doctor[/bold blue]", border_style="blue"))
    
    tools = {
        "Python": ["python", "--version"],
        "uv": ["uv", "--version"],
        "Docker": ["docker", "--version"],
        "Git": ["git", "--version"],
        "Terraform": ["terraform", "--version"]
    }
    
    for tool, cmd in tools.items():
        if check_command(cmd):
            console.print(f"[green][✓][/green] {tool} detectado no sistema.")
        else:
            console.print(f"[red][✗][/red] {tool} não encontrado no sistema.")
    
    console.print("\n[bold]Verificação concluída![/bold]")
