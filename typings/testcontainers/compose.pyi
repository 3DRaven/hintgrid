from typing import Self


class DockerCompose:
    """Docker Compose wrapper for test infrastructure."""

    def __init__(
        self,
        context: str,
        compose_file_name: str | list[str] | None = None,
        pull: bool = False,
        build: bool = False,
        wait: bool = True,
        env_file: str | None = None,
    ) -> None: ...

    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None: ...

    def get_service_port(self, service_name: str, port: int) -> str | None:
        """Get the exposed host port for a service's container port."""
        ...

    def get_service_host(self, service_name: str, port: int) -> str | None:
        """Get the host for a service."""
        ...

    def start(self) -> None:
        """Start all services."""
        ...

    def stop(self) -> None:
        """Stop all services."""
        ...

    def exec_in_container(
        self,
        service_name: str,
        command: list[str],
    ) -> tuple[int, bytes]:
        """Execute a command in a service container."""
        ...
