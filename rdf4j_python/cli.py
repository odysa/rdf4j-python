#!/usr/bin/env python3
"""
Interactive CLI for RDF4J Python

Provides a command-line interface for connecting to RDF4J repositories
and executing SPARQL queries with table-formatted results.
"""

import asyncio
import sys
from typing import Optional

from pyoxigraph import QuerySolutions, QueryTriples

from rdf4j_python import AsyncRdf4j


class RDF4JCLI:
    """Interactive CLI for RDF4J operations."""

    def __init__(self):
        self.db: Optional[AsyncRdf4j] = None
        self.repo = None
        self.server_url: Optional[str] = None
        self.repo_id: Optional[str] = None

    def print_welcome(self):
        """Print welcome message."""
        print("Welcome to RDF4J Python CLI")
        print("Type '\\h' for help, '\\q' to quit")
        print("Use '\\c <server_url> <repo_id>' to connect to a repository")
        print()

    def print_help(self):
        """Print help information."""
        print("Available commands:")
        print("  \\c <server_url> <repo_id>  - Connect to a repository")
        print("  \\h                          - Show this help")
        print("  \\q                          - Quit the CLI")
        print("  <SPARQL_query>              - Execute a SPARQL query (after connecting)")
        print()
        print("Examples:")
        print("  \\c http://localhost:19780/rdf4j-server my-repo")
        print("  SELECT * WHERE { ?s ?p ?o } LIMIT 10")
        print()

    async def connect_to_repository(self, server_url: str, repo_id: str) -> bool:
        """Connect to a repository."""
        try:
            # Close existing connection if any
            if self.db:
                await self.db.__aexit__(None, None, None)

            self.db = AsyncRdf4j(server_url)
            await self.db.__aenter__()
            self.repo = await self.db.get_repository(repo_id)
            self.server_url = server_url
            self.repo_id = repo_id
            
            print(f"✅ Connected to repository '{repo_id}' at {server_url}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to repository: {e}")
            return False

    def print_select_results(self, result: QuerySolutions, title: str = "Query Results"):
        """Print SELECT query results in a formatted table."""
        print(f"\n📊 {title}")
        print("=" * 60)

        # Convert to list to get count and iterate multiple times
        results_list = list(result)

        if not results_list:
            print("No results found.")
            return

        # Get variable names from the first result
        variables = list(results_list[0].keys())

        # Calculate column widths
        max_width = 25  # Maximum column width
        col_widths = {}
        for var in variables:
            max_var_width = max(
                len(var),
                max(
                    len(str(solution[var].value if solution[var] else 'NULL'))
                    for solution in results_list
                )
            )
            col_widths[var] = min(max_var_width, max_width)

        # Print header
        header = " | ".join(f"{var:<{col_widths[var]}}" for var in variables)
        print(header)
        print("-" * len(header))

        # Print rows
        for solution in results_list:
            row_parts = []
            for var in variables:
                value = solution[var].value if solution[var] else 'NULL'
                value_str = str(value)
                if len(value_str) > max_width:
                    value_str = value_str[:max_width-3] + "..."
                row_parts.append(f"{value_str:<{col_widths[var]}}")
            print(" | ".join(row_parts))

        print(f"\nTotal results: {len(results_list)}")

    def print_construct_results(self, result: QueryTriples, title: str = "Query Results"):
        """Print CONSTRUCT query results."""
        print(f"\n🔗 {title}")
        print("=" * 60)

        # Convert to list to count
        triples_list = list(result)

        if not triples_list:
            print("No triples found.")
            return

        for i, triple in enumerate(triples_list, 1):
            print(f"{i}. Subject:   {triple.subject}")
            print(f"   Predicate: {triple.predicate}")
            print(f"   Object:    {triple.object}")
            print()

        print(f"Total triples: {len(triples_list)}")

    async def execute_query(self, query: str):
        """Execute a SPARQL query and display results."""
        if not self.repo:
            print("❌ Not connected to any repository. Use \\c to connect first.")
            return

        try:
            print(f"Executing query: {query[:50]}{'...' if len(query) > 50 else ''}")
            result = await self.repo.query(query.strip())

            if isinstance(result, QuerySolutions):
                self.print_select_results(result)
            elif isinstance(result, QueryTriples):
                self.print_construct_results(result)
            elif isinstance(result, bool):
                # ASK query result
                print(f"\n❓ ASK Query Result: {'✅ True' if result else '❌ False'}")
            else:
                print(f"\n📄 Query Result: {result}")

        except Exception as e:
            print(f"❌ Query execution failed: {e}")

    async def run(self):
        """Run the interactive CLI."""
        self.print_welcome()

        try:
            while True:
                # Display prompt
                if self.repo_id:
                    prompt = f"rdf4j:{self.repo_id}> "
                else:
                    prompt = "rdf4j> "

                try:
                    user_input = input(prompt).strip()
                except (EOFError, KeyboardInterrupt):
                    print("\nGoodbye!")
                    break

                if not user_input:
                    continue

                # Handle commands
                if user_input.startswith('\\'):
                    command_parts = user_input.split()
                    command = command_parts[0]

                    if command == '\\q':
                        print("Goodbye!")
                        break
                    elif command == '\\h':
                        self.print_help()
                    elif command == '\\c':
                        if len(command_parts) != 3:
                            print("Usage: \\c <server_url> <repo_id>")
                            print("Example: \\c http://localhost:19780/rdf4j-server my-repo")
                        else:
                            server_url = command_parts[1]
                            repo_id = command_parts[2]
                            await self.connect_to_repository(server_url, repo_id)
                    else:
                        print(f"Unknown command: {command}")
                        self.print_help()
                else:
                    # Execute as SPARQL query
                    await self.execute_query(user_input)

        finally:
            # Clean up connection
            if self.db:
                await self.db.__aexit__(None, None, None)


def main():
    """Main entry point for the CLI."""
    cli = RDF4JCLI()
    try:
        asyncio.run(cli.run())
    except KeyboardInterrupt:
        print("\nGoodbye!")
        sys.exit(0)


if __name__ == "__main__":
    main()