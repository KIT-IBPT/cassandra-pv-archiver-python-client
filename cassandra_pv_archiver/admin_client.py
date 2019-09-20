"""
Web-service client for the administrative interface of the Cassandra PV
Archiver.
"""

import base64
import gzip
from http import HTTPStatus
import io
import json
import urllib.error
import urllib.request


class AdminClient(object):
    """
    Web-service client for the administrative interface of the Cassandra PV
    Archiver.
    """

    def __init__(self,
                 server_name,
                 server_port=4812,
                 username='admin',
                 password=''):
        """
        Create a web-service client.

        The web-service client is created for a specific server and with
        specific authentication credentials. After being created, it can be
        used for an arbitrary number of requests. It is designed to be safe for
        concurrent use by different threads.

        For most API functions, it does not matter to which server in a cluster
        the client connects. Each server can be used to make configuration
        changes affecting any other server. However, there might be a
        performance benefit when directly connecting to the right server.

        :param server_name:
            hostname or IP address of the Cassandra PV Archiver server to which
            the web-service client shall connect.
        :param server_port:
            port number on which the administrative interface of the Cassandra
            PV Archiver server is available. The default is 4812.
        :param username:
            username to be used for actions that require authentication. The
            default is "admin".
        :param password:
            password to be used for action that require authentication. The
            default is the empty string.
        """
        self._protocol_version = '1.0'
        self._base_url = 'http://{0}:{1}/admin/api/{2}'.format(
            server_name, server_port, self._protocol_version)
        self._username = username
        self._password = password
        self._auth_header = self._generate_auth_header()

    def export_server_configuration(self, server_id, configuration_file=None):
        """
        Export server configuration into a file.

        If no path to a configuration file is specified, the file contents are
        returned by this method (as binary data).

        This method raises an exception if it cannot get the configuration from
        the server or cannot write it to the specified file.

        :param server_id:
            UUID of the server for which the configuration shall be exported.
        :param configuration_file:
            path to the file into which the configuration shall be written. If
            ``None`` (the default), the configuration contents are returned by
            this method instead of writing them to a file.
        :return:
            configuration file contents if ``configuration_file`` is ``None``.
            ``None`` if the path to a configuration file is specified.
        """
        req = self._req('/channels/by-server/{0}/export'.format(server_id))
        resp_data = None  # Needed so that we do not get a warning.
        with self._do_req(req) as resp:
            status_code = resp.code
            if status_code == HTTPStatus.SERVICE_UNAVAILABLE:
                raise Exception('Service currently not available')
            elif not self._is_success_code(resp.code):
                raise Exception('Request failed with status code {0}'.format(
                    resp.code))
            resp_data = self._get_resp_data(resp)
        config_file_contents = base64.b64decode(
            resp_data['configurationFile'].encode(encoding='ascii'),
            validate=True)
        if configuration_file is None:
            return config_file_contents
        else:
            with io.BufferedWriter(open(configuration_file, 'wb')) as file:
                file.write(config_file_contents)
            return None

    def get_channel(self, channel_name, server_id=None):
        """
        Get configuration and status information for a channel.

        :param channel_name:
            name of the channel that should be queried.
        :param server_id:
            optional server ID. If specified, the channel information is only
            returned when the channel currently belongs to that server.
        :return:
            dictionary that is a verbatim copy of the server response (JSON
            converted to Python data-types).
        """
        channel_name = _encode_uri_part_custom(channel_name)
        if server_id is None:
            url = '/channels/all/by-name/{0}/'.format(channel_name)
        else:
            url = '/channels/by-server/{0}/by-name/{1}/'.format(
                server_id, channel_name)
        req = self._req(url)
        with self._do_req(req) as resp:
            if resp.code == HTTPStatus.SERVICE_UNAVAILABLE:
                raise Exception('Service currently not available')
            if not self._is_success_code(resp.code):
                raise Exception('Request failed with status code {0}'.format(
                    resp.code))
            return self._get_resp_data(resp)

    def get_cluster_status(self):
        """
        Get status information for the archive cluster.

        :return:
            dictionary that is a verbatim copy of the server response (JSON
            converted to Python data-types).
        """
        req = self._req('/cluster-status/')
        with self._do_req(req) as resp:
            if resp.code == HTTPStatus.SERVICE_UNAVAILABLE:
                raise Exception('Service currently not available')
            if not self._is_success_code(resp.code):
                raise Exception('Request failed with status code {0}'.format(
                    resp.code))
            return self._get_resp_data(resp)

    def get_server_status(self):
        """
        Get status information for the server.

        :return:
            dictionary that is a verbatim copy of the server response (JSON
            converted to Python data-types).
        """
        req = self._req('/server-status/this-server/')
        with self._do_req(req) as resp:
            if not self._is_success_code(resp.code):
                raise Exception('Request failed with status code {0}'.format(
                    resp.code))
            return self._get_resp_data(resp)

    def import_server_configuration(self,
                                    server_id,
                                    configuration_file,
                                    add_channels=True,
                                    remove_channels=False,
                                    update_channels=True,
                                    simulate=False):
        """
        Import a channel configuration file for a specific server.

        By default, this operation does not have any effects. In order to
        actually make changes, at least one of the possible operations
        (``add_channels``, ``remove_channels``, ``update_channels``) has to be
        enabled explicitly.

        The ``simulate`` flag can be set to simulate changes instead of
        actually applying them. This is particularly useful in combination with
        the ``remove_channels`` flag in order to find out which channels would
        be removed.

        If there is a general problem (e.g. a problem with the configuration
        file or a problem contacting the server), this method raises an
        exception. Problems only affecting individual channels, in contrast, do
        not result in an exception. Instead, an error message for the affected
        channels is returned as part of the result object.

        :param server_id:
            UUID of the server for which the configuration shall be imported.
        :param configuration_file:
            path to the configuration file that shall be imported.
            If this is a binary (bytes) object instead of a string, it is
            interpreted as the file contents and used directly instead.
        :param add_channels:
            add new channels to the server? If ``True``, channels that exist in
            the configuration file, but not on the server are added to the
            server. If ``False``, no new channels are added to the server and
            an error is generated for each channel that exists in the
            configuration file, but not on the server. Default is ``True``.
        :param remove_channels:
            remove channels from the server? If ``True``, channels that exist
            on the server, but not in the configuration file are removed from
            the server. If ``False``, no channels are removed from the server
            and channels on the server that are not mentioned in the
            configuration file are simply ignored. Default is ``False``.
        :param update_channels:
            update channel configuration on the server? If ``True``, channels
            that exist both in the configuration file and on the server are
            updated to match the configuration specified in the configuration
            file. If ``False``, channels that already exist on the server are
            not modified and an error is generated for each channel that exists
            both in the configuration file and on the server. Default is
            ``True``.
        :param simulate:
            simulate changes instead of applying them? If ``True``, the server
            configuration is not modified. Instead, the changes are simulated
            and the returned result is as if they had been applied. As the
            simulation is not complete, actually applying the changes might
            result in failures for channels that were reported as successful in
            the simulation.
        :return:
            dictionary that is a verbatim copy of the server response (JSON
            converted to Python data-types).
        """
        url = '/channels/by-server/{0}/import'.format(server_id)
        if isinstance(configuration_file, bytes):
            config_data = base64.b64encode(configuration_file)
        else:
            with open(configuration_file, mode='rb') as file:
                config_data = base64.b64encode(file.read())
        req_data = {
            'addChannels': add_channels,
            'configurationFile': config_data.decode(encoding='ascii'),
            'removeChannels': remove_channels,
            'simulate': simulate,
            'updateChannels': update_channels
        }
        req = self._req(url, req_data, method='POST', authenticate=True)
        with self._do_req(req) as resp:
            status_code = resp.code
            if status_code == HTTPStatus.FORBIDDEN:
                raise Exception('Authentication error')
            elif status_code == HTTPStatus.BAD_REQUEST:
                # noinspection PyBroadException
                try:
                    resp_data = self._get_resp_data(resp)
                    error_message = (resp_data['errorMessage']
                                     if 'errorMessage' in resp_data else None)
                except:
                    error_message = None
                raise Exception(error_message or 'Malformed request. Check '
                                                 'the input parameters')
            elif status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
                pass
            elif status_code == HTTPStatus.SERVICE_UNAVAILABLE:
                raise Exception('Service currently not available')
            elif not self._is_success_code(resp.code):
                raise Exception('Request failed with status code {0}'.format(
                    resp.code))
            resp_data = self._get_resp_data(resp)
            if ('errorMessage' in resp_data
                    and resp_data['errorMessage'] is not None):
                raise Exception(resp_data['errorMessage'])
            return resp_data

    def list_all_channels(self):
        """
        List all channels that exist in the cluster.

        The list returned by this method only contains very basic information
        about each channel. In order to get more detailed information, use the
        ``list_channels_for_server`` method.

        :return:
            list with an element for each channel. Each of the elements is a
            dictionary storing information about a single channel.
        """
        req = self._req('/channels/all/')
        with self._do_req(req) as resp:
            if resp.code == HTTPStatus.SERVICE_UNAVAILABLE:
                raise Exception('Service currently not available')
            if not self._is_success_code(resp.code):
                raise Exception('Request failed with status code {0}'.format(
                    resp.code))
            resp_data = self._get_resp_data(resp)
            return resp_data['channels']

    def list_channels_for_server(self, server_id):
        """
        List all channels for a specific server.

        If the server is online, the channel items contain status information
        in addition to the configuration information that is always returned.

        :param server_id:
            UUID of the server for which the channels shall be listed.
        :return:
            list with an element for each channel. Each of the elements is a
            dictionary storing information about a single channel.
        """
        req = self._req('/channels/by-server/{0}/'.format(server_id))
        with self._do_req(req) as resp:
            if resp.code == HTTPStatus.SERVICE_UNAVAILABLE:
                raise Exception('Service currently not available')
            if not self._is_success_code(resp.code):
                raise Exception('Request failed with status code {0}'.format(
                    resp.code))
            resp_data = self._get_resp_data(resp)
            return resp_data['channels']

    def run_archive_configuration_commands(self, commands):
        """
        Run a list of archive configuration commands.

        The commands may be executed in parallel and when all are finished, the
        result is returned.

        If there is a general problem (e.g. the database is not available or
        some of the commands are invalid), this method raises an exception.
        Problems only affecting individual commands, in contrast, do not result
        in an exception. Instead, an error message for the affected command is
        returned as part of the result object.

        :param commands:
            list of archive configuration commands. The easiest way of creating
            such a list is using the ``ArchiveConfigurationCommands`` class.
        :return:
            list that contains an element for each command. Each of these
            elements is a dict that represents the result of executing the
            command. This list has the same order as the commands in the list
            that was passed to this method.
            Each result object is a dict with the ``command``,
            ``errorMessage``, and ``success`` fields. The ``command`` is the
            command that was sent to the server, and ``success`` is a boolean
            that indicates whether the command executed successfully. If the
            command was not successful, the ``errorMessage`` might contain
            additional details. However, an error message is not guaranteed to
            be present, even if there was an error.
        """
        req_data = {
            'commands': commands
        }
        req = self._req('/run-archive-configuration-commands',
                        req_data, method='POST', authenticate=True)
        with self._do_req(req) as resp:
            status_code = resp.code
            if status_code == HTTPStatus.FORBIDDEN:
                raise Exception('Authentication error')
            elif status_code == HTTPStatus.BAD_REQUEST:
                # noinspection PyBroadException
                try:
                    resp_data = self._get_resp_data(resp)
                    error_message = (resp_data['errorMessage']
                                     if 'errorMessage' in resp_data else None)
                except:
                    error_message = None
                raise Exception(error_message or 'Malformed request. Check '
                                                 'the input parameters')
            elif status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
                pass
            elif status_code == HTTPStatus.SERVICE_UNAVAILABLE:
                raise Exception('Service currently not available')
            elif not self._is_success_code(resp.code):
                raise Exception('Request failed with status code {0}'.format(
                    resp.code))
            resp_data = self._get_resp_data(resp)
            if ('errorMessage' in resp_data
                    and resp_data['errorMessage'] is not None):
                raise Exception(resp_data['errorMessage'])
            return resp_data['results']

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

    def _generate_auth_header(self):
        """
        Generate and return the value for the ``Authorization`` header.
        """
        auth_data = base64.b64encode(
            '{0}:{1}'.format(self._username, self._password).encode())
        return 'Basic ' + auth_data.decode(encoding='ascii')

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


class ArchiveConfigurationCommands(list):
    """
    List of archive configuration commands.

    This class can be used for creating a list of commands that can then be
     passed to the ``run_archive_configuration_commands`` method of an
     ``AdminClient``.

    Commands can be appended to this list through the methods provided by this
    class. This class is derived from ``list`` so that all the methods expected
    for a list are available.
    """

    def __init__(self, *args, **kwargs):
        """
        Create a list of archive configuration commands.

        All parameters are directly passed on to the super-class constructor.
        This is mainly useful for creating a list that is prepopulated with the
        commands from another list.
        """
        super(ArchiveConfigurationCommands, self).__init__(*args, **kwargs)

    def add_channel(self,
                    channel_name,
                    control_system_type,
                    server_id,
                    decimation_levels=None,
                    decimation_level_to_retention_period=None,
                    enabled=True,
                    options=None):
        """
        Append an add channel command to this list.

        :param channel_name:
            name of the channel that shall be added.
        :param control_system_type:
            internal identifier for the control-system support that shall be
            used for the channel. This is the same identifier that is also used
            in configuration files.
        :param server_id:
            UUID of the server to which the channel shall be added.
        :param decimation_levels:
            decimation levels for the channel (identified by their decimation
            periods specified in seconds). The raw decimation level (with a
            decimation period of zero) is always added, even if it is not
            specified in the set. If the whole parameter is ``None``, this is
            interpreted like an set containing zero as its only element.
        :param decimation_level_to_retention_period:
            retention period for each decimation level (specified in seconds).
            The mapping uses the decimation period of the respective decimation
            level as the key and the retention period as the value of each
            pair. A value of zero means that samples shall be retained
            indefinitely. Entries for a decimation level that is not also
            specified in ``decimation_levels`` are discarded (except for the
            raw decimation level which is always included implicitly). Negative
            retention periods are converted to zero. If a decimation level is
            specified in ``decimation_levels``, but not in
            ``decimation_level_to_retention_period``, a retention period of
            zero is assumed. If the whole parameter is ``None`` (the default),
            a retention period of zero is used for all decimation levels.
        :param enabled:
            ``True`` if archiving shall be enabled for the channel (the
            default), `False`` if archiving shall be disabled.
        :param options:
            control-system-specific configuration options.
            The mapping uses the option name as the key and the option value as
            the value of each pair. If the whole parameter is ``None``  (the
            default), an empty mapping is used.
        """
        command = {
            'channelName': _make_str(channel_name),
            'commandType': 'add_channel',
            'controlSystemType': _make_str(control_system_type),
            'decimationLevels': _make_str_list(decimation_levels),
            'decimationLevelToRetentionPeriod':
                _make_str_dict(decimation_level_to_retention_period),
            'enabled': bool(enabled),
            'options': _make_str_dict(options),
            'serverId': _make_str(server_id)
        }
        self.append(command)

    def add_or_update_channel(self,
                              channel_name,
                              control_system_type,
                              server_id,
                              decimation_levels=None,
                              decimation_level_to_retention_period=None,
                              enabled=True,
                              options=None):
        """
        Append an add or update channel command to this list.

        :param channel_name:
            name of the channel that shall be added.
        :param control_system_type:
            internal identifier for the control-system support that shall be
            used for the channel. This is the same identifier that is also used
            in configuration files.
        :param server_id:
            UUID of the server to which the channel shall be added if it does
            not exist yet or to which it must belong if it already exists.
        :param decimation_levels:
            decimation levels for the channel (identified by their decimation
            periods specified in seconds). The raw decimation level (with a
            decimation period of zero) is always added, even if it is not
            specified in the set. If the whole parameter is ``None``, this is
            interpreted like an set containing zero as its only element.
        :param decimation_level_to_retention_period:
            retention period for each decimation level (specified in seconds).
            The mapping uses the decimation period of the respective decimation
            level as the key and the retention period as the value of each
            pair. A value of zero means that samples shall be retained
            indefinitely. Entries for a decimation level that is not also
            specified in ``decimation_levels`` are discarded (except for the
            raw decimation level which is always included implicitly). Negative
            retention periods are converted to zero. If a decimation level is
            specified in ``decimation_levels``, but not in
            ``decimation_level_to_retention_period``, a retention period of
            zero is assumed. If the whole parameter is ``None`` (the default),
            a retention period of zero is used for all decimation levels.
        :param enabled:
            ``True`` if archiving shall be enabled for the channel (the
            default), `False`` if archiving shall be disabled.
        :param options:
            control-system-specific configuration options.
            The mapping uses the option name as the key and the option value as
            the value of each pair. If the whole parameter is ``None``  (the
            default), an empty mapping is used.
        """
        command = {
            'channelName': _make_str(channel_name),
            'commandType': 'add_or_update_channel',
            'controlSystemType': _make_str(control_system_type),
            'decimationLevels': _make_str_list(decimation_levels),
            'decimationLevelToRetentionPeriod':
                _make_str_dict(decimation_level_to_retention_period),
            'enabled': bool(enabled),
            'options': _make_str_dict(options),
            'serverId': _make_str(server_id)
        }
        self.append(command)

    def move_channel(self,
                     channel_name,
                     new_server_id,
                     expected_old_server_id=None):
        """
        Append a move channel command to this list.

        :param channel_name:
            name of the channel that shall be moved.
        :param new_server_id:
            UUID of the server to which the channel shall be moved.
        :param expected_old_server_id:
            expected UUID of the server which currently owns the channel. If
            ``None`` (the default), the channel is moved regardless of the
            server it currently belongs to.
        """
        command = {
            'channelName': _make_str(channel_name),
            'commandType': 'move_channel',
            'expectedOldServerId': _make_str(expected_old_server_id),
            'newServerId': _make_str(new_server_id)
        }
        self.append(command)

    def refresh_channel(self,
                        channel_name,
                        server_id):
        """
        Append a refresh channel command to this list.

        :param channel_name:
            name of the channel that shall be refreshed.
        :param server_id:
            UUID of the server on which the refresh action should run. This
            does not have to be, but typically should be the UUID of the server
            that owns the channel.
        """
        command = {
            'channelName': _make_str(channel_name),
            'commandType': 'refresh_channel',
            'serverId': _make_str(server_id)
        }
        self.append(command)

    def remove_channel(self,
                       channel_name,
                       expected_server_id=None):
        """
        Append a remove channel command to this list.

        :param channel_name:
            name of the channel that shall be moved.
        :param expected_server_id:
            expected UUID of the server which currently owns the channel. If
            ``None`` (the default), the channel is removed regardless of the
            server it currently belongs to.
        """
        command = {
            'channelName': _make_str(channel_name),
            'commandType': 'remove_channel',
            'expectedServerId': _make_str(expected_server_id)
        }
        self.append(command)

    def rename_channel(self,
                       new_channel_name,
                       old_channel_name,
                       expected_server_id=None):
        """
        Append a rename channel command to this list.

        :param new_channel_name:
            new name for the channel.
        :param old_channel_name:
            old name of the channel.
        :param expected_server_id:
            expected UUID of the server which currently owns the channel. If
            ``None`` (the default), the channel is renamed regardless of the
            server it currently belongs to.
        """
        command = {
            'commandType': 'rename_channel',
            'expectedServerId': _make_str(expected_server_id),
            'newChannelName': _make_str(new_channel_name),
            'oldChannelName': _make_str(old_channel_name)
        }
        self.append(command)

    def update_channel(self,
                       channel_name,
                       add_decimation_levels=None,
                       add_options=None,
                       decimation_levels=None,
                       decimation_level_to_retention_period=None,
                       enabled=None,
                       expected_control_system_type=None,
                       expected_server_id=None,
                       options=None,
                       remove_decimation_levels=None,
                       remove_options=None):
        """
        Append an update channel command to this list.

        If ``add_decimation_levels`` or ``remove_decimation_levels`` is
        specified, ``decimation_levels`` must be ``None`` and the other way
        round.

        In the same way, if ``add_options`` or ``remove_options`` is specified,
        ``options`` must be ``None`` and the other way round.

        :param channel_name:
            name of the channel that shall be updated.
        :param add_decimation_levels:
            decimation levels (identified by their decimation periods specified
            in seconds) that shall be added for the channel. If the whole
            parameter is ``None`` (the default) and ``decimation_levels`` is
            also ``None``, no decimation levels are added.
        :param add_options:
            control-system-specific configuration options that shall be added
            to the channel configuration. This mapping uses the option name as
            the key and option value as the value of each pair. If one of the
            specified options already exists, its value is updated with the
            specified value. If this parameter is ``None`` (the default) and
            ``options`` is also ``None``,
            no control-system-specific options are added for the channel.
        :param decimation_levels:
            decimation levels for the channel (identified by their decimation
            periods specified in seconds). The raw decimation level (with a
            decimation period of zero) is always added, even if it is not
            specified in the set. If the whole parameter is ``None`` (the
            default) and ``add_decimation_levels`` and
            ``remove_decimation_levels`` are also ``None``, no decimation
            levels are added or removed. If the parameter is not ``None`` all
            existing decimation levels (except for the special raw decimation
            level) that are not also specified in the set are removed.
        :param decimation_level_to_retention_period:
            retention period for each decimation level (specified in seconds).
            The mapping uses the decimation period of the respective decimation
            level as the key and the retention period as the value of each
            pair. A value of zero means that samples shall be retained
            indefinitely. Entries for a decimation level that is not also
            specified in ``decimation_levels`` or ``add_decimation_levels`` and
            that does not exist, are discarded (except for the raw decimation
            level which is always included implicitly). Negative retention
            periods are converted to zero. If a decimation level is specified
            in ``decimation_levels`` or ``add_decimation_levels``, but not in
            ``decimation_level_to_retention_period``, a retention period of
            zero is assumed. If the whole parameter is ``None`` (the default),
            a retention period of zero is used for all decimation levels that
            are newly added and the retention periods of all other decimation
            levels are not changed.
        :param enabled:
            ``True`` if archiving shall be enabled for the channel (the
            default), `False`` if archiving shall be disabled. If ``None`` (the
            default),  the enabled flag is not changed.
        :param expected_control_system_type:
            internal identifier for the control-system support that is expected
            to be used for the channel. If ``None`` (the default), the channel
            is updated regardless of its control-system type.
        :param expected_server_id:
            expected UUID of the server which currently owns the channel. If
            ``None`` (the default), the channel is updated regardless of the
            server it currently belongs to.
        :param options:
            control-system-specific configuration options.
            The mapping uses the option name as the key and the option value as
            the value of each pair. If this parameter is specified, all options
            are replaced by the specified options, removing options that
            existed before, but were not specified. If the whole parameter is
            ``None``  (the default) and both ``add_options`` and
            ``remove_options`` are also ``None``, the control-system specific
            options for the channel are not changed.
        :param remove_decimation_levels:
            decimation levels (identified by their decimation periods specified
            in seconds) that shall be removed from the channel. If the whole
            parameter is ``None`` (the default) and ``decimation_levels`` is
            also ``None``, no decimation levels are removed. The special
            decimation level for raw samples is never removed, even if zero is
            an element of this set.
        :param remove_options:
            control-system-specific configuration options (identified by their
            names) that shall be removed from the channel configuration. If one
            of the specified options does not exist, it simply is not removed.
            If this parameter is ``None`` (the default) and ``options`` is also
            ``None``, no control-system-specific options are removed from the
            channel configuration.
        """
        command = {
            'addDecimationLevels': _make_str_list(add_decimation_levels),
            'addOptions': _make_str_dict(add_options),
            'channelName': _make_str(channel_name),
            'commandType': 'update_channel',
            'decimationLevels': _make_str_list(decimation_levels),
            'decimationLevelToRetentionPeriod':
                _make_str_dict(decimation_level_to_retention_period),
            'enabled': bool(enabled) if enabled is not None else None,
            'expectedControlSystemType':
                _make_str(expected_control_system_type),
            'expectedServerId': _make_str(expected_server_id),
            'options': _make_str_dict(options),
            'removeDecimationLevels': _make_str_list(remove_decimation_levels),
            'removeOptions': _make_str_list(remove_options)
        }
        self.append(command)


def _encode_uri_part_custom(uri_part):
    """
    Encode the URI part in the way expected by certain API functions.

    This is very similar to a regular URI encode, but encodes more characters
    and uses the tilde instead of the percent sign for escaping.
    :param uri_part:
        string to be encoded.
    :return:
        encoded string.
    """
    bin_data = uri_part.encode('utf_8')
    encoded_bin_data = bytearray()
    for b in bin_data:
        if (b == 0x2d or b == 0x5f or (0x30 <= b <= 0x39)
                or (0x41 <= b <= 0x5a) or (0x61 <= b <= 0x7a)):
            encoded_bin_data.append(b)
        else:
            # This code looks a bit strange, but effectively it converts a byte
            # to a three byte sequence, where the first byte is the ASCII code
            # for the tilde and the other two bytes represent a hexadecimal
            # number (in ASCII) that represents the value of the original byte.
            high = b // 16
            low = b % 16
            high_b = (0x30 + high) if high < 10 else (0x37 + high)
            low_b = (0x30 + low) if low < 10 else (0x37 + low)
            encoded_bin_data.append(0x7e)
            encoded_bin_data.append(high_b)
            encoded_bin_data.append(low_b)
    return encoded_bin_data.decode('ascii')


def _make_str(obj):
    """
    Convert the specified object to a string.

    If the object is ``None``, ``None`` is returned.
    :param obj: object that shall be converted to a string.
    :return: ``str(obj)`` or ``None`` if the object is ``None``.
    """
    return str(obj) if obj is not None else None


def _make_str_dict(dict_like_obj):
    """
    Convert a dict-like object to a dict of strings.

    This is mainly useful when creating objects for JSON serialization because
    the JSON serializer may expect dicts (instead of dict-like objects) and
    the API specification mandates using strings, even for numbers.

    :param dict_like_obj:
        object that is dict-like (has an ``items`` methods that returns an
        iterator over key-value pairs).
    :return:
        dict with key-value pairs from the passed object. The keys and values
        are converted to strings.
    """
    return {
        str(key): str(value) for key, value in dict_like_obj.items
        } if dict_like_obj is not None else None


def _make_str_list(list_like_obj):
    """
    Convert a list-like object to a dict of strings.

    This is mainly useful when creating objects for JSON serialization because
    the JSON serializer may expect lists (instead of list-like objects) and
    the API specification mandates using strings, even for numbers.

    :param list_like_obj:
        object that is list-like (provides an iterator over its elements).
    :return:
        list with elements from the passed object. The elements are converted
        to strings.
    """
    return [
        str(elem) for elem in list_like_obj
        ] if list_like_obj is not None else None
