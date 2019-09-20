"""
Web-service client for the administrative interface of the Cassandra PV
Archiver.
"""

import gzip
from http import HTTPStatus
import io
import json
import urllib.error
import urllib.parse
import urllib.request


class ArchiveClient(object):
    """
    Web-service client for accessing archive data stored with the Cassandra PV
    Archiver.
    """

    def __init__(self,
                 server_name,
                 server_port=9812):
        """
        Create a web-service client.

        The web-service client is created for a specific server. After being
        created, it can be used for an arbitrary number of requests. It is
        designed to be safe for concurrent use by different threads.

        For accessing the archive, it does not matter to which server in a
        cluster the client connects. Each server can be used to access the full
        archive.

        :param server_name:
            hostname or IP address of the Cassandra PV Archiver server to which
            the web-service client shall connect.
        :param server_port:
            port number on which the archive-access interface of the Cassandra
            PV Archiver server is available. The default is 9812.
        """
        self._protocol_version = '1.0'
        self._base_url = 'http://{0}:{1}/archive-access/api/{2}'.format(
            server_name, server_port, self._protocol_version)

    def find_channels_by_pattern(self, pattern):
        """
        Find and return channel names matching the specified pattern.

        The pattern must be a glob pattern, where "*" matches an arbitrary
        number of characters and "?" matches exactly one character.

        :param pattern: glob pattern to which channel names are matched.
        :return: list of channel names matching the pattern.
        """
        req_url = '/archive/1/channels-by-pattern/{0}' \
            .format(urllib.parse.quote(pattern, safe=''))
        req = self._req(req_url)
        with self._do_req(req) as resp:
            status_code = resp.code
            if status_code == HTTPStatus.SERVICE_UNAVAILABLE:
                raise Exception('Service currently not available')
            elif not self._is_success_code(resp.code):
                raise Exception('Request failed with status code {0}'.format(
                    resp.code))
            return self._get_resp_data(resp)

    def find_channels_by_regexp(self, regular_expression):
        """
        Find and return channel names matching the specified regular
        expression.

        The regular expression is interpreted by the server, so it must be a
        valid regular expression that is understood by Java.

        :param regular_expression: regular to which channel names are matched.
        :return: list of channel names matching the regular expression.
        """
        req_url = '/archive/1/channels-by-regexp/{0}' \
            .format(urllib.parse.quote(pattern, safe=''))
        req = self._req(req_url)
        with self._do_req(req) as resp:
            status_code = resp.code
            if status_code == HTTPStatus.SERVICE_UNAVAILABLE:
                raise Exception('Service currently not available')
            elif not self._is_success_code(resp.code):
                raise Exception('Request failed with status code {0}'.format(
                    resp.code))
            return self._get_resp_data(resp)

    def get_samples(self, channel_name, start_time, end_time, count=0):
        """
        Return the samples for the specified channel and time range.

        If there is no sample exactly matching the specified start time, one
        sample before the start time is included in the returned data if such a
        sample exists. In the same way, if there is no sample exactly matching
        the specified end time, one sample after the end time is included in
        the returned data if such a sample exists.

        The count parameter is optional. If a non-zero value is specified, the
        archive server selects the decimation level that will approximately
        contain the specified number of samples for the specified time range.

        :param channel_name: name of the channel for which data shall be
            returned.
        :param start_time: start time of the interval for which samples shall
            be returned. The start time is specified as the number of
            nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC).
        :param end_time: end time of the interval for which samples shall be
            returned. The end time is specified as the number of nanoseconds
            since epoch (January 1st, 1970, 00:00:00 UTC).
        :param count: approximate number of samples that shall be returned.
            If non-zero, the decimation level that is used is selected based on
            this number. If zero (the default), raw samples are returned.
        :return: array with samples as returned by the server.
        """
        req_url = '/archive/1/samples/{0}?start={1}&end={2}'\
            .format(urllib.parse.quote(channel_name, safe=''),
                    start_time,
                    end_time)
        if count > 0:
            req_url += '&count={0}'.format(count)
        req = self._req(req_url)
        with self._do_req(req) as resp:
            status_code = resp.code
            if status_code == HTTPStatus.SERVICE_UNAVAILABLE:
                raise Exception('Service currently not available')
            elif not self._is_success_code(resp.code):
                raise Exception('Request failed with status code {0}'.format(
                    resp.code))
            return self._get_resp_data(resp)

    @staticmethod
    def _do_req(req):
        """
        Send a request object and return the response. If an `HTTPError` is
        raised, it is caught and returned instead of the response object.
        """
        try:
            return urllib.request.urlopen(req)
        except urllib.error.HTTPError as err:
            return err

    @staticmethod
    def _get_content_type_and_charset(resp):
        """
        Extract and return the content type and (optionally) the charset from a
        response's ``Content-Type`` header.
        """
        content_type_header = resp.headers.get('Content-Type', None)
        content_type_header = content_type_header.split(';')
        content_type = content_type_header[0]
        extra_args = {}
        for extra_arg in content_type_header[1:]:
            extra_arg = extra_arg.split('=', 1)
            if len(extra_arg) == 2:
                extra_args[extra_arg[0]] = extra_arg[1]
            else:
                extra_args[extra_arg[0]] = None
        charset = extra_args.get('charset', None)
        return content_type, charset

    def _get_resp_data(self, resp):
        """
        Read and return JSON data from a response.

        Raises an exception if the response does not have the expected content
        type (``application/json``).
        """
        content_type, charset = self._get_content_type_and_charset(resp)
        if content_type != 'application/json':
            raise Exception(
                'Expected content-type application/json, but got {0}.'.format(
                    content_type))
        # If the charset is not specified, we assume UTF-8 (actually JSON
        # should always use UTF-8).
        if charset is None:
            charset = 'utf_8'
        content_encoding = resp.headers.get('Content-Encoding', None)
        if content_encoding == 'gzip':
            file_object = gzip.GzipFile(fileobj=resp)
        else:
            file_object = resp
        return json.load(io.TextIOWrapper(file_object, encoding=charset))

    @staticmethod
    def _is_success_code(status_code):
        """
        Tells whether the specified HTTP status code indicates success. All
        status codes between 200 and 299 are considered as successful.
        """
        return (status_code >= 200) and (status_code < 300)

    # noinspection PyDefaultArgument
    def _req(self,
             url,
             data=None,
             headers={},
             method='GET',
             authenticate=False):
        """
        Creates and returns a request object.

        This method takes care of converting the supplied data object to JSON,
        setting the appropriate request headers and including an authorization
        header (if requested).
        """
        req_url = self._base_url + url
        if data is not None:
            req_data = json.dumps(data).encode()
        else:
            req_data = None
        req_headers = {
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip'
        }
        req_headers.update(headers)
        if authenticate:
            req_headers['Authorization'] = self._auth_header
        if req_data is not None:
            req_headers['Content-Type'] = 'application/json;charset=UTF-8'
        return urllib.request.Request(
            req_url, req_data, req_headers, method=method)
