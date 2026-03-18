"""
Tableau Cloud publisher.
Authenticates with a Personal Access Token (PAT) and publishes (overwrites)
the Workday .hyper file as a Published Data Source on Tableau Cloud.

PublishMode.Overwrite preserves all existing workbook connections — dashboards
continue working and automatically reflect the new data.
"""
from __future__ import annotations

from pathlib import Path

import tableauserverclient as TSC
from loguru import logger

from etl.config import TableauSettings


class TableauCloudPublisher:
    def __init__(self, settings: TableauSettings) -> None:
        self.settings = settings
        self._auth = TSC.PersonalAccessTokenAuth(
            token_name=settings.token_name,
            personal_access_token=settings.token_value,
            site_id=settings.site_id,
        )
        self._server = TSC.Server(settings.server_url, use_server_version=True)

    def publish(self, hyper_path: Path) -> str:
        """
        Publish (overwrite) the .hyper file to Tableau Cloud.
        Returns the published data source ID.
        """
        logger.info(
            f"Connecting to Tableau Cloud: {self.settings.server_url} "
            f"(site={self.settings.site_id})"
        )

        with self._server.auth.sign_in(self._auth):
            project_id = self._resolve_project(self.settings.project_name)

            datasource_item = TSC.DatasourceItem(
                project_id=project_id,
                name=self.settings.datasource_name,
            )

            logger.info(
                f"Publishing '{self.settings.datasource_name}' to project "
                f"'{self.settings.project_name}' (Overwrite mode) ..."
            )

            result = self._server.datasources.publish(
                datasource_item,
                str(hyper_path),
                TSC.Server.PublishMode.Overwrite,
            )

            logger.info(
                f"Published successfully. Datasource id={result.id}, "
                f"updated_at={result.updated_at}"
            )
            return result.id

    def _resolve_project(self, project_name: str) -> str:
        """Return the project ID for the given project name."""
        request_options = TSC.RequestOptions()
        request_options.filter.add(
            TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, project_name)
        )
        projects, _ = self._server.projects.get(request_options)

        if not projects:
            raise ValueError(
                f"Project '{project_name}' not found on Tableau Cloud site "
                f"'{self.settings.site_id}'. Check TABLEAU_PROJECT_NAME in .env."
            )
        return projects[0].id
