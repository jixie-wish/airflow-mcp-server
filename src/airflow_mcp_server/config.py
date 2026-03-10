class AirflowConfig:
    """Centralized configuration for Airflow MCP server."""

    def __init__(
        self,
        base_url: str | None = None,
        auth_token: str | None = None,
    ) -> None:
        """Initialize configuration with provided values.

        Args:
            base_url: Airflow API base URL
            auth_token: Authentication token (JWT)

        Raises:
            ValueError: If required configuration is missing
        """
        if not base_url or not base_url.strip():
            raise ValueError("Missing required configuration: base_url")
        # aiohttp requires base_url to end with '/' (e.g. http://localhost:9080/api/v1/)
        self.base_url = base_url.strip().rstrip("/") + "/"

        self.auth_token = auth_token
        if not self.auth_token:
            raise ValueError("Missing required configuration: auth_token (JWT)")
