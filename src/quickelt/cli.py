import typer

from quickelt.commands import doctor, init

app = typer.Typer(
    name="quickelt",
    help="QuickELT CLI",
    no_args_is_help=True,
)

app.command(name="doctor")(doctor.doctor_command)
app.command(name="init")(init.init_command)

if __name__ == "__main__":
    app()
