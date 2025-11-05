import logging

from ibmcloudant import CouchDbSessionAuthenticator, cloudant_v1

logger = logging.getLogger(__name__)


class StatusdbSession:
    """Wrapper class for couchdb."""

    def __init__(self, config):
        user = config.get("username")
        password = config.get("password")
        url = config.get("url")
        display_url_string = f"https://{user}:********@{url}"
        self.connection = cloudant_v1.CloudantV1(
            authenticator=CouchDbSessionAuthenticator(user, password)
        )
        self.connection.set_service_url(f"https://{url}")
        try:
            server_info = self.connection.get_server_information().get_result()
            if not server_info:
                raise Exception(f"Connection failed for URL {display_url_string}")
        except Exception as e:
            raise Exception(
                f"Couchdb connection failed for URL {display_url_string} with error: {e}"
            )

    def get_db_doc(self, run_id):
        pass

    def update_db_doc(self):
        pass
