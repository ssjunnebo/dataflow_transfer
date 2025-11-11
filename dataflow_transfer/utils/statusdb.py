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
        self.db_name = config.get("database")
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

    def get_db_doc(self, ddoc, view, run_id):
        doc_id = self.get_doc_id(ddoc, view, run_id)
        if doc_id:
            return self.connection.get_document(
                db=self.db_name, doc_id=doc_id
            ).get_result()
        return None

    def get_doc_id(self, ddoc, view, run_id):
        result = self.connection.post_view(
            db=self.db_name,
            ddoc=ddoc,
            view=view,
            key=run_id,
        ).get_result()
        if result and "rows" in result and len(result["rows"]) > 0:
            return result["rows"][0]["id"]
        else:
            return None

    def get_events(self, run_id):
        return self.connection.post_view(
            db=self.db_name,
            ddoc="events",
            view="current_status_per_runfolder",
            key=run_id,
        ).get_result()

    def update_db_doc(self, db_doc):
        # Upload document to the database. Retry upload 3 times before failing.
        for attempt in range(3):
            try:
                self.connection.post_document(
                    db=self.db_name, document=db_doc
                ).get_result()
                return
            except Exception as e:
                logger.error(
                    f"Attempt {attempt + 1} to update document {db_doc.get('runfolder_id', 'unknown')} failed with error: {e}"
                )
        raise Exception(
            f"Failed to update document {db_doc.get('runfolder_id', 'unknown')} after 3 attempts."
        )
