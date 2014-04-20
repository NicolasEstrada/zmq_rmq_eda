import argparse
import os
import subprocess
import sys
import time

import yaml

from python_utils import logging_manager


INFINITE_RETRIES = -1

DEFAULT_POLLING_TIME = 10
DEFAULT_PYTHON_BIN = "python"
DEFAULT_KEEP_ALIVE = True
DEFAULT_RESPAWN_RETRIES = INFINITE_RETRIES  # This means "infinite"

MIN_UPTIME_THRESHOLD = 60
RESPAWN_WARNING_THRESHOLD = 2


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="""Start a group of processes and manage them.
        The flags set default values in case they are not present in the
        config file.""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('config', help='Config file to load')

    parser.add_argument(
        '--polling',
        default=DEFAULT_POLLING_TIME,
        type=int,
        help='Polling time (in seconds)')

    parser.add_argument(
        '--python-bin',
        default=DEFAULT_PYTHON_BIN,
        help='Python binary to use')
    parser.add_argument(
        '--max-respawn-retries',
        default=DEFAULT_RESPAWN_RETRIES,
        type=int,
        help="""Maximum number of respawn retries per process.
        "-1" means no limit""")
    parser.add_argument(
        '--respawn-warning',
        default=RESPAWN_WARNING_THRESHOLD,
        type=int,
        help="""Amount of respawns considered to raise an alarm. This is done
        to detect processes that are respawned too much times""")
    parser.add_argument(
        '--min-uptime',
        default=MIN_UPTIME_THRESHOLD,
        type=int,
        help="""Minimum amount of seconds that a processes should be running.
        If a process dies before this time, a warning is raised. This is done
        to detect processes that die too soon""")

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--keep-alive',
        action='store_true',
        dest='keep_alive',
        default=DEFAULT_KEEP_ALIVE,
        help="Keep alive the processes")
    group.add_argument(
        '--no-keep-alive',
        action='store_false',
        dest='keep_alive',
        default=(not DEFAULT_KEEP_ALIVE),
        help="Don't try to keep alive the processes")

    args, _ = parser.parse_known_args()

    with open(args.config) as file_r:
        CONFIG = yaml.load(file_r)


def any_running(processes):
    for group in processes.itervalues():
        for job in group['running_jobs']:
            if job['process'].poll() is None:
                return True

    return False


def check_dead_processes(group_name, group):
    respawned_count = 0
    dead_count = 0

    # Check the dead processes
    for job in group["running_jobs"]:
        # If the process is not flagged as "failed permanently"
        # but we found that the process returned an exit code
        job_exit_code = job["process"].poll()

        if (not job["failed_permanently"] and job_exit_code is not None):
            # Check if the job exceeded the amount of respawns
            if group["max_respawn_retries"] == INFINITE_RETRIES:
                not_excessive_respawns = True
            else:
                not_excessive_respawns = (
                    job["respawn_retries"] < group["max_respawn_retries"])

            # If we need to keep the job alive and is not a "respawn aggressor"
            if (group["keep_alive"] and not_excessive_respawns):

                job_params = job["parameters"]

                if isinstance(job_params, list):
                    if job_params:
                        print_nice_params = " ".join(job_params)
                    else:
                        print_nice_params = ""
                else:
                    print_nice_params = job_params

                logger.info("Respawing Job {} [{}] (Exit Code {})".format(
                    job["id"],
                    "{} {}".format(
                        group["script"], print_nice_params),
                    job_exit_code))

                # WARNING: This metric is not accurate!
                now = time.time()
                uptime = now - job["last_start_at"]

                uptime_warning = (uptime <= MIN_UPTIME_THRESHOLD)
                respawn_warning = (
                    job["respawn_retries"] > RESPAWN_WARNING_THRESHOLD)

                if uptime_warning and respawn_warning:
                    logger.warning(
                        "{} [{}]: Job {} respawned {} times. Last uptime: {} seconds".format(
                            group_name,
                            group["id"],
                            job["id"],
                            job["respawn_retries"],
                            uptime))

                elif uptime_warning:
                    logger.warning(
                        "{} [{}]: Job {} died early. Last uptime: ~{} seconds".format(
                            group_name,
                            group["id"],
                            job["id"],
                            uptime))

                elif respawn_warning:
                    logger.warning(
                        "{} [{}]: Job {} respawned {} times in {} seconds".format(
                            group_name,
                            group["id"],
                            job["id"],
                            job["respawn_retries"],
                            (now - job["created_at"])))

                timestamp = time.time()

                job["process"] = new_instance(
                    group["script"],
                    group["python_exec"],
                    parameters=job["parameters"])

                job["last_start_at"] = timestamp
                job["last_updated_at"] = timestamp
                job["respawn_retries"] += 1
                job["uptime"] = 0

                respawned_count += 1

            # Here we just leave the poor bastard dead
            else:
                logger.info(
                    "{} [{}]: Job {} failed permanently (Exit Code {})".format(
                        group_name,
                        group["id"],
                        job["id"],
                        job_exit_code))

                job["failed_permanently"] = True
                job["last_updated_at"] = time.time()

                dead_count += 1

        # If the job keeps running
        else:
            timestamp = time.time()
            job["uptime"] = (timestamp - job["last_start_at"])
            job["last_updated_at"] = timestamp

    return respawned_count, dead_count


def perform():
    processes = {}

    polling_time = CONFIG.get('polling_time', args.polling)
    python_exec = CONFIG.get('python_exec', args.python_bin)
    keep_alive = CONFIG.get('keep_alive', args.keep_alive)
    max_respawn_retries = CONFIG.get(
        'respawn_retries',
        args.max_respawn_retries)
    alert_on_error = CONFIG.get('alert_on_error')

    # Start the processes
    for group_id, (group_name, group_definition) in enumerate(
            CONFIG['processes'].iteritems(), start=1):

        instances = None

        # First, we check the type of the 'instances' parameter
        group_instances = group_definition.get('instances')

        if group_instances is not None:
            if isinstance(group_instances, list):
                # If it's a list, it indicates that len(list) instances
                # should be launched.
                total_instances = len(group_instances)

                # Each element of the list defines one of the instances
                instances = tuple(group_instances)
            else:
                # Otherwise, the 'instances' parameter is the number of
                # instances that should be launched.
                total_instances = group_instances
        else:
            total_instances = 1

        # If instances aren't set
        if instances is None:

            # It means that all the instances uses the same parameters
            # The parameters are the same for all the instances
            group_parameters = group_definition.get('parameters')

            if group_parameters is not None:
                parameters = tuple(group_parameters)
            else:
                parameters = tuple()

            # TEST THIS!
            instances = [parameters] * total_instances

        # The group should be keeped alive?
        group_keep_alive = group_definition.get('keep_alive', keep_alive)

        # How much times a process of the group should be respawned?
        group_max_respawn_retries = group_definition.get(
            'max_respawn_retries',
            max_respawn_retries)

        # List of emails to alert when something goes wrong
        group_alert_on_error = group_definition.get(
            'alert_on_error',
            alert_on_error)

        # Default Python executable
        group_python_exec = group_definition.get('python_exec', python_exec)

        # Check for changes in the script?
        # if 'check_for_changes' in group_definition:
        #     check_for_changes = group_definition['check_for_changes']
        # else:
        #     check_for_changes = DEFAULT_CHECK_FOR_CHANGES

        script_full_path = os.path.abspath(group_definition['script'])
        group_alias = group_definition.get('name', group_name)

        # And now, the group definition is stored
        processes[group_name] = process_group = dict(
            id=group_id,
            name=group_alias,
            script=script_full_path,
            number_of_instances=total_instances,
            instances=instances,
            keep_alive=group_keep_alive,
            max_respawn_retries=group_max_respawn_retries,
            python_exec=group_python_exec,
            alert_on_error=group_alert_on_error,
            running_jobs=[],
            dead_jobs=[])

        # The creation of the processes starts
        # For each group of parameters...
        for instance_id, instance_parameters in enumerate(
                process_group["instances"], start=1):

            # A new process is instatiated
            timestamp = time.time()

            job = dict(
                id=((group_id * 100) + instance_id),
                created_at=timestamp,
                uptime=0,
                respawn_retries=0,
                last_start_at=timestamp,
                last_updated_at=timestamp,
                failed_permanently=False,
                parameters=instance_parameters,
                process=new_instance(
                    process_group["script"],
                    process_group["python_exec"],
                    parameters=instance_parameters))

            # And adding that job to the running jobs of the group
            process_group["running_jobs"].append(job)

    logger.info("Waiting 5 seconds for processes status consistency")
    time.sleep(5)

    # Before starting the polling loop, check if there's
    # some processes to watch. Sometimes they die at
    # startup time and we don't know yet
    if not any_running(processes):
        logger.critical("All jobs already finished. Terminating")
        return

    # And the polling starts!
    while True:
        logger.info('Polling')

        try:
            pong = time.time()

            for group_name, group in processes.iteritems():
                respawned_count = 0
                failed_count = 0

                logger.debug("Polling group {} [{}]".format(
                    group_name,
                    group['id']))

                if group['running_jobs']:
                    respawned_count, failed_count = check_dead_processes(
                        group_name,
                        group)

                    # Updates the list of running processes
                    group["dead_jobs"] = [job for job
                                          in group["running_jobs"]
                                          if job["failed_permanently"]]

                    group["running_jobs"] = [job for job
                                             in group["running_jobs"]
                                             if not job["failed_permanently"]]

                # Here the notation is
                # R: Running
                # S: Spawned (Resurrected)
                # F: Failed Permanently (this cycle)
                # D: Total Dead Jobs
                logger.info((
                    "Summary for {} [{}]: R: {} | S: {} | F: {} | D: {}"
                    .format(
                        group_name,
                        group['id'],
                        len(group["running_jobs"]),
                        respawned_count,
                        failed_count,
                        len(group["dead_jobs"]))))

            ping = time.time()
            diff = ping - pong

            if diff < polling_time:
                # Waits if necessary
                sleep_time = polling_time - diff

                logger.debug('Sleeping {0} seconds'.format(sleep_time))
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Keyboard Interrupt... finishing")
            break
        except Exception:
            logger.exception('An uncaught exception has ocurred')
            break


def new_instance(script, python_executable, parameters=None):

    init_values = [python_executable, script]

    if parameters is not None:
        if isinstance(parameters, basestring):
            parameters = parameters.split()

        init_values.extend(parameters)

    try:
        return subprocess.Popen(init_values)
    except OSError:
        logger.exception("Error creating a new process")
        raise
    except Exception:
        logger.exception("Uncaught exception")
        raise


def pid_file():
    pid_file = CONFIG['pid_file']

    if os.path.isfile(pid_file):
        logger.critical("{0} already exists, exiting".format(pid_file))
        sys.exit()
    else:
        with open(os.path.join(pid_file), 'w') as new_pid:
            new_pid.write("{0}".format(os.getpid()))


if __name__ == '__main__':
    logger = logging_manager.start_logger('watcher', use_root_logger=False)

    pid_file()

    try:
        logger.info("Starting to watch processes")
        perform()
    except Exception:
        logger.exception("An uncaught exception has ocurred")
        raise
    finally:
        os.remove(CONFIG['pid_file'])
        logger.info("Watcher finished his work... good bye!")
