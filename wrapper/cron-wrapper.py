import os, datetime
import argparse
import json
import uuid
import subprocess
import logging
import filelock

LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s - %(message)s'
STDOUT_OUTPUT_FILE = None

DEBUG = False
VERBOSE = False
DEFAULT_OUTPUT_FOLDER = '/var/cron-wrapper/data'
DEFAULT_AGGREGATED_FILENAME = 'cron.json'
AGGREGATION_LOCK_ACQ_TIMEOUT = 10
SAVE_EXECUTION_UUID = False

if STDOUT_OUTPUT_FILE is None:
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
else:
    logging.basicConfig(
        filename=STDOUT_OUTPUT_FILE,
        filemode='a',
        format=LOG_FORMAT, 
        level=logging.INFO)

logger = logging.getLogger('cron-wrapper')
logger.setLevel(logging.INFO)
logging.getLogger('filelock').setLevel(logging.WARN)

parser = argparse.ArgumentParser(description='Run a CRON job and capture information in JSON files.')
parser.add_argument('name', help='the unique name for the job')
parser.add_argument('command', help='the command to run')
parser.add_argument('--timeout', '-t', type=int, required=False, default=60, help='timeout in seconds')
parser.add_argument('--force', '-f', required=False, default=False, help='force execution if another instance is running', action='store_const', const=True)
parser.add_argument('--verbose', '-v', required=False, default=False, help='show verbose log', action='store_const', const=True)
parser.add_argument('--output-folder', required=False, default=DEFAULT_OUTPUT_FOLDER, help='output folder for status file')
parser.add_argument('--output-file', '-o', required=False, default=None, help='output status file')
parser.add_argument('--report-stdout', required=False, default=False, help='report stdout in status file', action='store_const', const=True)
parser.add_argument('--report-previous', required=False, default=False, help='report previous info in status file', action='store_const', const=True)
parser.add_argument('--skip-write', required=False, default=False, help='do not write to output file', action='store_const', const=True)
parser.add_argument('--skip-read', required=False, default=False, help='do not read latest status from output file', action='store_const', const=True)
parser.add_argument('--skip-aggregation', required=False, default=False, help='do not write to aggregated output file', action='store_const', const=True)
parser.add_argument('--debug', required=False, default=False, help='run in debug mode with additional output', action='store_const', const=True)

args = parser.parse_args()

if args.debug:
    DEBUG = True
    logger.info('running in debug mode')

if DEBUG or args.verbose:
    logger.info('running in verbose mode')
    VERBOSE = True

if VERBOSE:
    logger.setLevel(logging.DEBUG)
    logging.getLogger('filelock').setLevel(logging.DEBUG)
    logger.debug('running cron wrapper [py] with args: %s', repr(args))

if args.force:
    logger.warn('using --force may cause corruption of the job data if multiple instances are running at the same time.')

output_aggregation_file = args.output_folder + '/' + DEFAULT_AGGREGATED_FILENAME
output_aggregation_lock = filelock.FileLock(output_aggregation_file + '.lock')

if (args.output_file is not None):
    output_file = args.output_file
else:
    output_file = args.output_folder + '/' + args.name + '.json'

if not args.skip_write:
    logger.debug('writing output to %s', output_file)
    logger.debug('writing aggregated output to %s', output_aggregation_file)

if (not args.skip_read) and os.path.exists(output_file):
    logger.debug('importing latest status from %s', output_file)
    with open(output_file, 'r') as infile:
        latest_status = json.load(infile)
        if 'previous' in latest_status:
            del latest_status['previous']
else:
    latest_status = None

execution_number = 1
if latest_status is not None:
    if 'executionNumber' in latest_status:
        execution_number = latest_status['executionNumber'] + 1

    # check if another instance running
    if 'status' in latest_status and latest_status['status'] == 'RUNNING':
        latest_expires_at = datetime.datetime.strptime(latest_status['expiresAt'], '%Y-%m-%dT%H:%M:%S.%f')
        logger.warn('another instance for the same job is already running, expiring %s', latest_expires_at.isoformat())
        if latest_expires_at < datetime.datetime.now():
            logger.info('executing because other instance is EXPIRED')
        elif not args.force:
            logger.info('quitting without executing')
            exit(4)
        else:
            logger.warn('executing because FORCE is enabled')

started_at = datetime.datetime.now()

def runcommand (cmd):
    logger.info('running command "%s"', cmd)
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True,
                            universal_newlines=True)
    std_out, std_err = proc.communicate(timeout=args.timeout)
    if std_out is not None:
        std_out = std_out.strip()
    if std_err is not None:
        std_err = std_err.strip()
    return proc.returncode, std_out, std_err

status = dict()
if args.report_previous:
    status['previous'] = latest_status

if SAVE_EXECUTION_UUID:
    status['executionId'] = str(uuid.uuid4())

status['executionNumber'] = execution_number
status['status'] = 'RUNNING'
status['startedAt'] = started_at.isoformat()
if DEBUG:
    status['user'] = runcommand('whoami')[1]
    status['workingDirectory'] = runcommand('pwd')[1]
status['expiresAt'] = (started_at + datetime.timedelta(0, args.timeout)).isoformat()

def write_output():
    tmp_filename = output_file + "~" + str(uuid.uuid4())
    logger.debug('writing status to temp file %s', tmp_filename)
    with open(tmp_filename, 'w') as tmp_file:
        json.dump(status, tmp_file)
    logger.debug('persisting status from temp file to status file %s', output_file)
    os.rename(tmp_filename, output_file)

def write_aggregated_output(payload):
    try:
        with output_aggregation_lock.acquire(timeout = AGGREGATION_LOCK_ACQ_TIMEOUT):      
            tmp_filename = output_aggregation_file + "~" + str(uuid.uuid4())
            logger.debug('writing status to aggregated temp file %s', tmp_filename)
            with open(tmp_filename, 'w') as tmp_file:
                json.dump(payload, tmp_file)
            logger.debug('persisting aggregated status from temp file to aggregated status file %s', output_aggregation_file)
            os.rename(tmp_filename, output_aggregation_file)
    except filelock.Timeout as e:
        # could not acquire lock for write back
        logger.error('failed to acquire lock for aggregation write', exc_info=True)
        raise e

def read_aggregated_status():
    if not os.path.exists(output_aggregation_file):
        return None
    data = None
    try:
        with output_aggregation_lock.acquire(timeout = AGGREGATION_LOCK_ACQ_TIMEOUT):            
            with open(output_aggregation_file, 'r') as infile:
                data = json.load(infile)
        return data
    except filelock.Timeout as e:
        # could not acquire lock for read
        logger.error('failed to acquire lock for aggregation read', exc_info=True)
        raise e

def update_aggregated_output():
    try:
        logger.debug('acquiring lock on aggregated output')
        with output_aggregation_lock.acquire(timeout = AGGREGATION_LOCK_ACQ_TIMEOUT):
            logger.debug('acquired lock on aggregated output')
            agg_data = dict()
            # read aggregated data
            read_data = read_aggregated_status()
            if read_data is not None:
                agg_data = read_data

            # merge with agg_data
            agg_data[args.name] = status

            # write to aggregated status file
            write_aggregated_output(agg_data)

    except filelock.Timeout:
        # could not acquire lock
        logger.error('failed to acquire lock for aggregation update', exc_info=True)

    except Exception:
        # could not acquire lock
        logger.error('failed to write aggregated status', exc_info=True)

def update_status():
    logger.debug('updating status to %s', status['status'])
    if VERBOSE:
        logger.debug('STATUS UPDATE %s', json.dumps(status, indent=2))
    if not args.skip_write:
        write_output()
        if not args.skip_aggregation:
            update_aggregated_output()

update_status()

success = None

cmd = args.command
exit_code = 0
timed_out = False
try:
    result = runcommand(cmd)
    returncode = result[0]
    std_out = result[1]
    std_err = result[2]
    exit_code = returncode
    status['returnCode'] = returncode
    if args.report_stdout:
        status['stdOut'] = std_out
        status['stdErr'] = std_err
    if returncode > 0:
        success = False
        status['error'] = std_err
        status['errorDetails'] = std_err
    else:
        success = True
except subprocess.TimeoutExpired as e:
    logger.error('EXECUTION TIMED OUT', exc_info=True)
    timed_out = True
    exit_code = 2
    success = False
    status['error'] = str(e)
    status['errorDetails'] = repr(e)
except Exception as e:
    logger.error('LAUNCH FAILED', exc_info=True)
    exit_code = 3
    success = False
    status['error'] = str(e)
    status['errorDetails'] = repr(e)

finished_at = datetime.datetime.now()

if success:
    status['status'] = 'FINISHED'
else:
    logger.error('EXECUTION FAILED')
    status['status'] = 'FAILED'

status['timedOut'] = timed_out
status['finishedAt'] = finished_at.isoformat()
status['duration'] = (finished_at - started_at).total_seconds()

status['success'] = success
update_status()
logger.info('exiting with code %d', exit_code)
exit(exit_code)