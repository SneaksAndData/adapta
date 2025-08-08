"""Requests module"""
import os
from urllib.request import url2pathname

import requests
from requests.adapters import BaseAdapter


class LocalFileAdapter(BaseAdapter):
    """Protocol Adapter to allow Requests to GET file:// URLs"""

    @staticmethod
    def _validate_local_path(method, path) -> tuple[int, str]:
        """Return an HTTP status for the given filesystem path."""
        if method.lower() in ("put", "delete"):
            return 501, "Not Implemented"
        if method.lower() not in ("get", "head"):
            return 405, "Method Not Allowed"
        if os.path.isdir(path):
            return 400, "Path Not A File"
        if not os.path.isfile(path):
            return 404, "File Not Found"
        if not os.access(path, os.R_OK):
            return 403, "Access Denied"
        return 200, "OK"

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        path = os.path.normcase(os.path.normpath(url2pathname(request.path_url)))
        response = requests.Response()

        response.status_code, response.reason = self._validate_local_path(request.method, path)
        if response.status_code == 200 and request.method.lower() != "head":
            if response.status_code == 200 and request.method.lower() != "head":
                try:
                    with open(path, "rb") as file:
                        response._content = file.read()
                        response.raw = None
                except OSError as err:
                    response.status_code = 500
                    response.reason = str(err)

        if isinstance(request.url, bytes):
            response.url = request.url.decode("utf-8")
        else:
            response.url = request.url

        response.request = request
        response.connection = self

        return response

    def close(self):
        pass
