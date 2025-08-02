"""CLI module for rdf4j-python."""

import asyncio
import sys
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

from rdf4j_python import AsyncRdf4j
from rdf4j_python.exception.repo_exception import RepositoryCreationException
from rdf4j_python.model._repository_info import RepositoryMetadata

console = Console()


class RDF4JCLIContext:
    """Context object to store CLI state."""
    
    def __init__(self):
        self.rdf4j_client: Optional[AsyncRdf4j] = None
        self.server_url: Optional[str] = None
        self.connected: bool = False


@click.command()
@click.argument('server_url')
def main(server_url: str):
    """RDF4J CLI - A psql-like interface for RDF4J servers.
    
    Connect to a RDF4J server and enter interactive mode.
    
    Args:
        server_url: URL of the RDF4J server (e.g., http://localhost:7200/rdf4j-server)
    """
    console.print(f"Connecting to RDF4J server: {server_url}", style="blue")
    
    ctx = RDF4JCLIContext()
    ctx.server_url = server_url
    
    # Test connection
    async def test_connection():
        try:
            async with AsyncRdf4j(server_url) as rdf4j:
                version = await rdf4j.get_protocol_version()
                ctx.connected = True
                console.print("Connected!", style="green bold")
                console.print(f"Protocol version: {version}", style="dim")
                return rdf4j
        except Exception as e:
            console.print(f"Connection failed: {e}", style="red bold")
            sys.exit(1)
    
    # Run the connection test
    try:
        asyncio.run(test_connection())
    except KeyboardInterrupt:
        console.print("\nConnection cancelled.", style="yellow")
        sys.exit(0)
    
    # Enter interactive mode
    interactive_mode(ctx)


def interactive_mode(ctx: RDF4JCLIContext):
    """Run the interactive CLI mode."""
    console.print("\nEntering interactive mode. Type '\\help' for available commands.", style="dim")
    console.print("Type '\\quit' or Ctrl+C to exit.\n", style="dim")
    
    while True:
        try:
            # Display prompt
            command = console.input("[bold blue]>[/bold blue] ").strip()
            
            if not command:
                continue
                
            if command in ['\\quit', '\\q', 'exit']:
                console.print("Goodbye!", style="yellow")
                break
            elif command in ['\\help', '\\h']:
                show_help()
            elif command in ['\\ls', '\\list']:
                asyncio.run(list_repositories(ctx))
            elif command in ['\\status', '\\s']:
                show_status(ctx)
            else:
                console.print(f"Unknown command: {command}", style="red")
                console.print("Type '\\help' for available commands.", style="dim")
                
        except KeyboardInterrupt:
            console.print("\nGoodbye!", style="yellow")
            break
        except EOFError:
            console.print("\nGoodbye!", style="yellow")
            break


def show_help():
    """Display help information."""
    help_table = Table(title="Available Commands", style="cyan")
    help_table.add_column("Command", style="bold")
    help_table.add_column("Description")
    
    help_table.add_row("\\ls, \\list", "List all repositories")
    help_table.add_row("\\status, \\s", "Show connection status")
    help_table.add_row("\\help, \\h", "Show this help message")
    help_table.add_row("\\quit, \\q", "Exit the CLI")
    
    console.print(help_table)


def show_status(ctx: RDF4JCLIContext):
    """Show connection status."""
    status_table = Table(title="Connection Status")
    status_table.add_column("Property", style="bold")
    status_table.add_column("Value")
    
    status_table.add_row("Server URL", ctx.server_url or "Not set")
    status_table.add_row("Connected", "Yes" if ctx.connected else "No")
    
    console.print(status_table)


async def list_repositories(ctx: RDF4JCLIContext):
    """List all repositories on the RDF4J server."""
    if not ctx.server_url:
        console.print("No server connected.", style="red")
        return
    
    try:
        async with AsyncRdf4j(ctx.server_url) as rdf4j:
            repositories = await rdf4j.list_repositories()
            
            if not repositories:
                console.print("No repositories found.", style="yellow")
                return
            
            # Create a table for repositories
            repo_table = Table(title="Repositories")
            repo_table.add_column("ID", style="bold cyan")
            repo_table.add_column("Title", style="green")
            repo_table.add_column("URI", style="blue")
            repo_table.add_column("Readable", justify="center")
            repo_table.add_column("Writable", justify="center")
            
            for repo in repositories:
                readable = "✓" if repo.readable else "✗"
                writable = "✓" if repo.writable else "✗"
                
                repo_table.add_row(
                    repo.id,
                    repo.title,
                    repo.uri,
                    readable,
                    writable
                )
            
            console.print(repo_table)
            
    except Exception as e:
        console.print(f"Error listing repositories: {e}", style="red")


if __name__ == "__main__":
    main()