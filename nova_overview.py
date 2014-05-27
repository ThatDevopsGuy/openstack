#!/usr/bin/env python
# coding: utf-8

'''
===================================================================================================
                                                                _
                        _ _  _____ ____ _   _____ _____ _ ___ _(_)_____ __ __
                       | ' \/ _ \ V / _` | / _ \ V / -_) '_\ V / / -_) V  V /
                       |_||_\___/\_/\__,_| \___/\_/\___|_|  \_/|_\___|\_/\_/

                                           nova overview
===================================================================================================

Script:         nova_overview.py
Author:         Sebastian Weigand
Email:          sab@swdd.io
Current:        May, 2014 
Version:        2.10.2 (bad ips, column fix)
Description:    Shows an overview of instances and their hypervisors.

===================================================================================================
'''

try:
    from MySQLdb import connect, cursors
    from prettytable import PrettyTable
    import simplejson as json
    from ping import quiet_ping
    from multiprocessing import Pool
    from time import sleep
    from os import popen, getuid
    from sys import stderr
    from socket import gaierror, gethostbyname, gethostbyaddr, herror, setdefaulttimeout, timeout
    import argparse

except ImportError as e:
    print e
    print 'You need that Python module.'
    print 'Try running `pip install ping prettytable MySQL-python`'
    exit(1)

# =============================================================================
# Libs & Init
# =============================================================================

parser = argparse.ArgumentParser(description='Get instance and hypervisor state information based on inputs.')
parser.add_argument('-e', '--env', type=str, help='show only this environment')
parser.add_argument('-n', '--name', type=str, help='show only instances matching this (fuzzy) name')
parser.add_argument('-p', '--project', type=str, help='show only this project (a.k.a. tenant)')
parser.add_argument('-u', '--user', type=str, help='show only instances made by this user')
parser.add_argument('-y', '--hypervisors', type=str, nargs='+', help='restrict output to only these (fuzzy) hypervisors')
parser.add_argument('-s', '--sort-by', type=str, help='specify a column to sort by [Hostname]')
parser.add_argument('--uuid', action="store_true", default=False, help='show the instance UUID')
parser.add_argument('--ping', action="store_true", default=False, help='provide availability information (requires root)')
parser.add_argument('--check-dns', action="store_true", default=False, help='test DNS resolution against IP')
parser.add_argument('--bad-ips', action="store_true", default=False, help='show only VMs without IP addresses')
parser.add_argument('--show-query', action="store_true", default=False, help='show the corresponding SQL query for your parameters')
parser.add_argument('--html', action="store_true", default=False, help='output table in HTML format')
parser.add_argument('--version', action='version', version='%(prog)s 2.10')
args = parser.parse_args()

if (args.ping and args.bad_ips) or (args.check_dns and args.bad_ips):
    exit('Cannot perform networking tasks on instances without IPs.')

if args.ping:
    if getuid() != 0:
        exit('Run me as root to enable ICMP echo requests...')

if args.check_dns:
    setdefaulttimeout(1)

# =============================================================================


def do_sql(query, variables=None, db='nova'):
    '''Returns a resultDict for a given sql query.'''

    # Must be called and connected every query time, otherwise the MySQL
    #   server will have 'gone away' from stale connections:

    db_mappings = {
        'nova': {
            'server': '<NOVA MYSQL SERVER HERE>',
            'password': '<SOME PASSWORD>'
        },

        'keystone': {
            'server': '<KEYSTONE MYSQL SERVER HERE>',
            'password': '<SOME PASSWORD>'
        }
    }

    conn = connect(host=db_mappings[db]['server'],
                   user=db,
                   db=db,
                   passwd=db_mappings[db]['password']
                   )

    cursor = conn.cursor(cursors.DictCursor)

    if variables:
        cursor.execute(query, variables)
    else:
        cursor.execute(query)

    result_dict = cursor.fetchall()
    conn.close()
    return result_dict

# =============================================================================

# Terminal colors:
red = '\033[91m'
green = '\033[92m'
end = '\033[0m'

# =============================================================================

dns_suffix = '.example.com'


def get_table_row(record):
    ''' Get an array to use with PrettyTable.addrow() '''

    # Some records will lack any network information in the JSON object:
    if record['network_info'] == '[]':
        _ip = '-'

        if args.ping:
            _ping_message = '-'

        if args.check_dns:
            _dns_message = '-'
            _rdns_message = '-'

    else:
        _ip = json.loads(record['network_info'])[0]['network']['subnets'][0]['ips'][0]['address']

        if args.ping:
            _results = quiet_ping(_ip, timeout=2, count=10)
            _ping_message = ''
            if _results:
                if _results[0] > 0:
                    if _results[0] == 100:
                        _ping_message = '%sDOWN%s' % (red, end)
                    else:
                        _ping_message = '%s!!%s %s%% loss (%sms, %sms)' % (red, end, _results[0], round(_results[1], 2), round(_results[2], 2))
                else:
                    _ping_message = '%sOK%s (%sms, %sms)' % (green, end, round(_results[1], 2), round(_results[2], 2))
            else:
                _ping_message = '%sDOWN%s' % (red, end)

        if args.check_dns:
            _name = record['hostname'] + dns_suffix
            try:
                _resolved_ip = gethostbyname(_name)

            except gaierror:
                _resolved_ip = None

            except timeout:
                _resolved_ip = 'timed out'

            try:
                _resolved_name = gethostbyaddr(_ip)[0]

            except herror:
                _resolved_name = None

            except timeout:
                _resolved_name = 'timed out'

            # DNS:
            if _resolved_ip == _ip:
                _dns_message = '%s%s%s' % (green, _resolved_ip, end)

            elif _resolved_ip is None:
                _dns_message = '%s!! No DNS record !! %s' % (red, end)

            else:
                _dns_message = '%s!! %s !!%s' % (red, _resolved_ip, end)

            # Reverse DNS:
            if _resolved_name == _name:
                _rdns_message = '%s%s%s' % (green, _resolved_name.replace(dns_suffix, ''), end)

            elif _resolved_name is None:
                _rdns_message = '%s!! No RDNS record !! %s' % (red, end)

            else:
                _rdns_message = '%s!! %s !!%s' % (red, _resolved_name.replace(dns_suffix, ''), end)

    _row = [
        record['hostname'],
        _ip,
        record['user_id'],
        record['created_at'],
        project_id_mappings[record['project_id']],
        record['name'],  # Is the instance flavor name
        record['host'].replace(dns_suffix, '')
    ]

    if args.ping:
        _row.insert(2, _ping_message)

    if args.uuid:
        _row.insert(4, record['uuid'])

    if args.check_dns:
        _row.insert(2, _dns_message)
        _row.insert(3, _rdns_message)

    if disabled_hypervisors:
        _disabled_reason = ''
        if record['disabled'] == 0:
            _status = 'Enabled'
        else:
            _status = 'Disabled'

        if not record['disabled_reason']:
            _disabled_reason = ''

        _row.extend([_status, _disabled_reason])

    return _row

# =============================================================================
# Main
# =============================================================================

# Get a simple dict for project ID lookups:
project_id_mappings = {}
# And make it hash-friendly:
for record in do_sql('select id, name from project', db='keystone'):
    project_id_mappings[record['id']] = record['name']
    # Do 2-way searching, as it's only a handful of records:
    project_id_mappings[record['name']] = record['id']

uber_query = '''
select  instances.hostname, 
        instances.uuid,
        instances.user_id,
        instances.project_id,
        instances.created_at,
        services.host, 
        services.disabled, 
        services.disabled_reason,
        instance_info_caches.network_info,
        instance_types.name
from    services
join instances on services.host=instances.host 
join instance_info_caches on instance_info_caches.instance_uuid=instances.uuid
join instance_system_metadata on instance_system_metadata.instance_uuid=instances.uuid
join instance_types on instance_system_metadata.value=instance_types.flavorid
where instances.vm_state = "active" and
instance_types.deleted_at is null and
instance_system_metadata.key = "instance_type_flavorid"'''

# Mix in custom filters for the SQL:

# This array must be constructed in the same order as additional filters are added,
# such that the variables passed to the SQL connector line-up:
sql_variables = []

if args.name:
    uber_query += '\nand instances.hostname like %s'
    sql_variables.append('%' + args.name + '%')

if args.env:
    uber_query += '\nand instances.hostname like %s'
    sql_variables.append(args.env + '%')

if args.project:
    uber_query += '\nand instances.project_id = %s'
    sql_variables.append(project_id_mappings[args.project])

if args.user:
    uber_query += '\nand instances.user_id = %s'
    sql_variables.append(args.user)

if args.hypervisors:
    uber_query += '\nand (services.host like %s'
    uber_query += '\nor services.host like %s' * (len(args.hypervisors) - 1)
    uber_query += ')'
    sql_variables.extend(['%%%s%%' % hypervisor for hypervisor in args.hypervisors])

# Final sorting parameter:
uber_query += ' \norder by instances.hostname;'

if len(sql_variables) > 0:
    uber_table = do_sql(uber_query, sql_variables)
else:
    uber_table = do_sql(uber_query)

if args.show_query:
    print ''
    print '=' * 80
    print uber_query % tuple(sql_variables), '\n'
    print '=' * 80
    print ''

if len(uber_table) == 0:
    print 'Nothing matched your query.'
    if not args.show_query:
        print 'Try again with "--show-query" to see the corresponding SQL.'
    exit()

# Do we need to include 2 additional columns?:
disabled_hypervisors = any([x['disabled'] for x in uber_table])

table_fields = ['Hostname', 'IP Address', 'DNS Status', 'RDNS Status', 'Instance Status', 'Creator', 'Created At', 'UUID', 'Project',
                'Flavor', 'Hypervisor', 'Hypervisor Status', 'Status Reason']

if not disabled_hypervisors:
    table_fields.remove('Hypervisor Status')
    table_fields.remove('Status Reason')

if not args.ping:
    table_fields.remove('Instance Status')

if not args.uuid:
    table_fields.remove('UUID')

if not args.check_dns:
    table_fields.remove('DNS Status')
    table_fields.remove('RDNS Status')

pt = PrettyTable(table_fields)

if args.sort_by:
    if args.sort_by not in table_fields:
        exit('Cannot sort by "%s".\nMust be one of: "%s"' % (args.sort_by, '", "'.join(table_fields)))

# =============================================================================
# Multi-process stuff
# =============================================================================

if args.ping or args.check_dns:
    pool = Pool(20)
    rows = pool.map_async(get_table_row, uber_table)

    poll_time = 1
    previous_w = 0

    stderr.write('Gathering instance networking information on %s nodes...' % len(uber_table))
    stderr.flush()

    # Print out a nice progress bar and spinner thingy:
    while True:
        progress = int(((len(uber_table) - rows._number_left) / float(len(uber_table))) * 100)
        # Set the time to wait above, in seconds:
        for i in xrange(poll_time):
            w = int(popen('stty size', 'r').read().split()[1])

            for char in ['/', '-', '\\', '|']:
                line = '['
                bar_length = int((w - 8) * (progress / 100.0))
                line += '=' * bar_length
                if progress == 100:
                    line += '='
                else:
                    line += char
                line = line.ljust(w - 8)
                line += '] ' + str(progress).rjust(3) + '%'

                if w != previous_w:
                    print ''
                    previous_w = w

                stderr.write('\b' * w)  # Clear the line backwards
                stderr.write(line)      # Write the line
                stderr.flush()          # Flush the line
                sleep(0.25)             # Sleep 1/4 of a second, for 4 chars

            if progress == 100:
                break

        if progress == 100:
            break

    # Keep this map here, as rows done async needs .get():
    map(pt.add_row, rows.get())

else:
    # Keep this map here:
    rows = map(get_table_row, uber_table)

    if args.bad_ips:
        rows = [row for row in rows if row[1] == '-']

    map(pt.add_row, rows)

# =============================================================================
# Print stuff
# =============================================================================

pt.align = 'l'

if __name__ == "__main__":
    if args.sort_by:
        pt.sortby = args.sort_by
    elif args.hypervisors:
        pt.sortby = 'Hypervisor'

    if args.html:
        print pt.get_html_string()
    else:
        print pt
