"""
Command Line Interface for SQL RAG application.
"""

import logging
import sys
from typing import Dict, Any

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
import inquirer

from sql_rag.main import SQLRagApplication

logger = logging.getLogger(__name__)
console = Console()

def display_welcome_message():
    """Display a welcome message for the CLI interface."""
    console.print(Panel.fit(
        "[bold blue]SQL RAG CLI Interface[/bold blue]\n\n"
        "Query SQL databases and ask natural language questions",
        title="Welcome",
        border_style="blue"
    ))

def display_help():
    """Display help information."""
    help_table = Table(show_header=True, header_style="bold magenta")
    help_table.add_column("Command", style="dim")
    help_table.add_column("Description")
    
    help_table.add_row("sql <query>", "Execute SQL query and store results in vector database")
    help_table.add_row("ask <question>", "Ask a natural language question about your data")
    help_table.add_row("tables", "List available tables in the database")
    help_table.add_row("schema <table>", "Show schema of a specific table")
    help_table.add_row("help", "Show this help message")
    help_table.add_row("exit/quit", "Exit the application")
    
    console.print(Panel(help_table, title="Available Commands", border_style="blue"))

def prompt_for_database_connection(config):
    """Prompt user for database connection details."""
    console.print("\n[bold]Database Connection Setup[/bold]")
    
    questions = [
        inquirer.Text('server', message="Server address", default=config["database"].get("server", "localhost")),
        inquirer.Text('database', message="Database name", default=config["database"].get("database", "")),
        inquirer.Confirm('use_windows_auth', message="Use Windows Authentication?", default=config["database"].getboolean("use_windows_auth", True)),
    ]
    
    answers = inquirer.prompt(questions)
    if not answers:
        return False
    
    if not answers['use_windows_auth']:
        username = Prompt.ask("Username", default=config["database"].get("username", ""))
        password = Prompt.ask("Password", password=True)
        answers['username'] = username
        answers['password'] = password
    
    # Update config
    for key, value in answers.items():
        config["database"][key] = str(value)
    
    # Save the updated config
    with open("config.ini", "w") as f:
        config.write(f)
    
    return True

def display_tables(app):
    """Display available tables in the database."""
    try:
        df = app.sql_connector.get_tables()
        if df.empty:
            console.print("[yellow]No tables found in the database[/yellow]")
            return
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Schema")
        table.add_column("Table Name")
        table.add_column("Type")
        
        for _, row in df.iterrows():
            table.add_row(
                row['schema_name'],
                row['table_name'],
                row['table_type']
            )
        
        console.print(Panel(table, title="Available Tables", border_style="blue"))
    except Exception as e:
        console.print(f"[red]Error listing tables: {e}[/red]")

def display_schema(app, table_name, schema='dbo'):
    """Display schema for a specific table."""
    try:
        if '.' in table_name:
            schema, table_name = table_name.split('.')
        
        df = app.sql_connector.get_columns(table_name, schema)
        if df.empty:
            console.print(f"[yellow]No columns found for table {schema}.{table_name}[/yellow]")
            return
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Column Name")
        table.add_column("Data Type")
        table.add_column("Max Length")
        table.add_column("Nullable")
        
        for _, row in df.iterrows():
            table.add_row(
                row['column_name'],
                row['data_type'],
                str(row['max_length']) if row['max_length'] else '-',
                row['is_nullable']
            )
        
        console.print(Panel(table, title=f"Schema for {schema}.{table_name}", border_style="blue"))
    except Exception as e:
        console.print(f"[red]Error showing schema: {e}[/red]")

def start_cli(config):
    """Start the command-line interface."""
    # Initialize the application
    display_welcome_message()
    
    try:
        app = SQLRagApplication(config_path="config.ini")
        app.ensure_collection_exists()
    except Exception as e:
        console.print(f"[red]Error initializing application: {e}[/red]")
        
        # Prompt for database connection details
        if "database connection" in str(e).lower():
            if prompt_for_database_connection(config):
                try:
                    app = SQLRagApplication(config_path="config.ini")
                    app.ensure_collection_exists()
                except Exception as e:
                    console.print(f"[red]Error initializing application after reconfiguration: {e}[/red]")
                    return
            else:
                return
    
    # Main CLI loop
    while True:
        try:
            user_input = Prompt.ask("\n[bold green]SQL RAG[/bold green]", default="help")
            command = user_input.strip()
            
            if command.lower() in ('exit', 'quit'):
                console.print("[yellow]Exiting SQL RAG CLI...[/yellow]")
                break
                
            elif command.lower() == 'help':
                display_help()
            
            elif command.lower() == 'tables':
                display_tables(app)
            
            elif command.lower().startswith('schema '):
                table_name = command[7:].strip()
                if not table_name:
                    console.print("[yellow]Please specify a table name: schema <table_name>[/yellow]")
                else:
                    display_schema(app, table_name)
            
            elif command.lower().startswith('sql '):
                sql_query = command[4:].strip()
                if not sql_query:
                    console.print("[yellow]Please provide a SQL query[/yellow]")
                    continue
                
                console.print(f"[blue]Executing SQL query:[/blue] {sql_query}")
                with console.status("[bold green]Running query and storing results...[/bold green]"):
                    try:
                        app.store_sql_data_in_vector_db(sql_query)
                        console.print("[green]SQL query results stored in vector database[/green]")
                    except Exception as e:
                        console.print(f"[red]Error executing SQL query: {e}[/red]")
            
            elif command.lower().startswith('ask '):
                question = command[4:].strip()
                if not question:
                    console.print("[yellow]Please provide a question[/yellow]")
                    continue
                
                console.print(f"[blue]Processing question:[/blue] {question}")
                with console.status("[bold green]Searching for relevant information...[/bold green]"):
                    try:
                        result = app.run_rag_query(question)
                        
                        # Display retrieved data
                        console.print("\n[bold]Retrieved Information:[/bold]")
                        for i, item in enumerate(result["retrieved_data"]):
                            console.print(Panel(
                                f"[bold]Score:[/bold] {item['score']:.4f}\n\n{item['payload']['text']}",
                                title=f"Result {i+1}",
                                border_style="blue"
                            ))
                        
                        # Display response
                        console.print(Panel(result["response"], title="Response", border_style="green"))
                    except Exception as e:
                        console.print(f"[red]Error processing question: {e}[/red]")
            
            else:
                console.print("[yellow]Unknown command. Type 'help' for available commands.[/yellow]")
                
        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted. Use 'exit' to quit.[/yellow]")
        except EOFError:
            console.print("\n[yellow]Exiting SQL RAG CLI...[/yellow]")
            break
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")

if __name__ == "__main__":
    # This allows running the CLI directly for testing
    import configparser
    config = configparser.ConfigParser()
    config.read("config.ini")
    start_cli(config)