Cassandra PV Archiver Python Client
===================================

This Python library provides an easy way of using the webservice interfaces
of a Cassandra PV Archiver server instance. There are two separate modules
for the two different kinds of webservices offered by the server:

This library has been written for use with Python 3 and will most likely not
work with Python 2.

The `cassandra_pv_archiver.admin_client` module is designed to interact with the
administrative API of the server, while the
`cassandra_pv_archiver.archive_client` is designed to facilitate user access to
the archive.

Administrative client
---------------------

In order to use the administrative API, an instance of the `AdminClient` class
has to be instantiated:

```
from cassandra_pv_archiver.admin_client import AdminClient

client = AdminClient('myserver.example.com', password='mysecret')
```

If the server does not use the standard port (4812) for the administrative
interface or if the username is not "admin", further parameters have to be
passed to the `AdminClient` constructor. Please refer to the API documentation
inside the source of the module for details.

Once the client object has been created, it can be used to interact with the
server, like in the following examples. Again, to learn more about the full
range of parameters available, please refer to the API documentation inside the
source code. You might also find the
[documentation of the webservice API](http://oss.aquenos.com/cassandra-pv-archiver/docs/3.2.5/manual/html/apc.html)
useful, because it contains detailed information about the structure of some of
the result objects.

### Exporting a server configuration

The configuration for a server can be exported to a file like this:

```
client.export_server_configuration(
    '82c711df-632b-49a3-85c2-d3249555eb57', 'path/to/target_file.xml')
```

This is equivalent to using the export function in the web UI.

### Getting information about a channel

The current configuration and status of a channel can be retrieved like this:

```
channel_info = client.get_channel('my_channel')
print(channel_info)
```

### Getting the cluster status

The current status of the cluster can be queried like this:

```
cluster_status = client.get_cluster_status()
print(cluster_status)
```

### Getting the server status

The current status of the server that is being queried can be returned like
this:

```
server_status = client.get_server_status()
print(server_status)
```

Due to limitations of the webservice API it is currently not possible to get the
status of a different server than the one that client is connected to.

### Importing a server configuration

A configuration file (in the same format as the ones returned by the export
function) can be imported like this:

```
result = client.import_server_configuration(
    '82c711df-632b-49a3-85c2-d3249555eb57',
    'path/to/source_file.xml',
    add_channels=True,
    remove_channels=False,
    update_channels=True,
    simulate=False)
print(result)
```

The `add_channels`, `remove_channels`, and `update_channels` flags indicate
whether channels present in the imported file, but not the current
configuration, should be added to the server configuration, whether channels
present in the current configuration, but not in the imported file, should be
removed from the server configuration, and whether channels present in both the
imported file and the current configuration should be updated with the
configuration from the file.

If the `simulate` flag is set, the changes are not actually made, but the
returned result will indicate which changes would have been made.

### Listing all channels

A list with all channels that currently exist in the cluster can be retrieved
like this:

```
channels = client.list_all_channels()
print('There are {} channels.'.format(len(channels)))
for channel in channels:
    print(
        'This is the information for channel {}:'
        .format(channel['channelName']))
    print(channel)
```

### Listing all channels of a server

A list with all channels that exist on a specific server can be retrieved like
this:

```
channels = client.list_channels_for_server(
    '82c711df-632b-49a3-85c2-d3249555eb57')
print('The server has {} channels.'.format(len(channels)))
for channel in channels:
    print(
        'This is the information for channel {}:'
        .format(channel['channelName']))
    print(channel)
```

In contrast to the result of `list_all_channels`, the objects returned by this
method contain additional status information for each channel.

### Making individual changes to the configuration

It is possible to apply a set of changes to the cluster configuration. In order
to do this, one must first assemble a list of the commands that shall be
applied. The commands in this list can affect the configuration of different
servers, so for commands where the affected server is not identified by the
channel, the server ID needs to be specified. For other commands, the server ID
can still be specified in order to ensure that the command is only applied if
the channel actually belongs to that server.

```
from cassandra_pv_archiver.admin_client import ArchiveConfigurationCommands

commands = ArchiveConfigurationCommands()

commands.add_channel(
    'new_channel', 'channel_access', '82c711df-632b-49a3-85c2-d3249555eb57')

commands.add_or_update_channel(
    'channel_that_might_already_exist',
    'channel_access',
    '82c711df-632b-49a3-85c2-d3249555eb57',
    enabled=False)

commands.move_channel(
    'channel_to_be_moved',
    '82c711df-632b-49a3-85c2-d3249555eb57',
    expected_old_server_id='51093923-45fa-4b5b-8476-4bb0238a2da8')

commands.refresh_channel('some_channel')

commands.remove_channel('channel_to_be_deleted')

commands.rename_channel('new_channel_name', 'old_channel_name')

commands.update_channel('existing_channel', decimation_levels=[0, 30, 500])

result = client.run_archive_configuration_commands(commands)
print(result)
```

The result is a list that contains exactly one entry for each command,
reflecting the result of the execution of that specific command.

Archive client
--------------

In order to use the archive API, an instance of the `ArchiveClient` class has to
be instantiated:

```
from cassandra_pv_archiver.archive_client import ArchiveClient

client = ArchiveClient('myserver.example.com')
```

If the server does not use the standard port (9812) for the archive access,
further parameters have to be passed to the `ArchiveClient` constructor. Please
refer to the API documentation inside the source of the module for details.

Once the client object has been created, it can be used to interact with the
server, like in the following examples. Again, to learn more about the full
range of parameters available, please refer to the API documentation inside the
source code. You might also find the
[documentation of the webservice API](http://oss.aquenos.com/cassandra-pv-archiver/docs/3.2.5/manual/html/apb.html)
useful, because it contains detailed information about the structure of some of
the result objects.

### Finding channels matching a certain pattern

It is possible to retrieve a list of channels with names that match a certain
glob pattern like this:

```
channels = client.find_channels_by_pattern('my_prefix:*')
print(channels)
```

### Finding channels matching a regular expression

It is possible to retrieve a list of channels with names that match a certain
regular expression like this:

```
channels = client.find_channels_by_regexp('my_prefix:[0-9]+:.*')
print(channels)
```

### Retrieving samples

It is possible to retrieve archived samples for a channel. Please note that the
start and end of the interval have to be specified as the number of nanoseconds
since January 1st, 1970, 00:00:00 UTC.

```
samples = client.get_samples(
    'my_channel', 1567823452000000000, 1568967971000000000)
```

It is possible to retrieve decimated samples (if available for a channel) by
indicating the count of desired samples. The server will select the decimation
level that gives a result with the number of samples being as close to the
specified number as possible:

```
samples = client.get_samples(
    'my_channel', 1567823452000000000, 1568967971000000000, count=600)
```

License
-------

The Cassandra PV Archiver Python client is licensed under the terms of the GNU
Lesser General Public License version 3. Please refer to the
[license text](LICENSE.txt) and the [licensing notices](NOTICE.txt) for details.
