import os
import logging
from logging.handlers import TimedRotatingFileHandler
import inspect
import collections
import hashlib

import bunch
from redis import StrictRedis
from rq import Queue
from rq.job import Job as RQJob

def wrapper_task():
    pass

def load_yaml_config(envvar):
    """Load a yaml-formatted config file.

    Examples:
        >>> cfg = load_yaml_config('PROJECT_CONFIG')
        >>> cfg.project_name
        Cool Project
        >>> cfg.db.host
        127.0.01.1

    Args:
        envvar (str): The environment variable pointing to the config file.

    Returns:
        dict: A dictionary-like object allowing attribute-style access to
            configuration parameters.
    """
    infile = os.environ.get(envvar)
    if not infile:
        # click.secho('{} env variable is not set.'.format(envvar), fg='red')
        raise SystemExit
    else:
        with open(infile, 'r') as f:
            return bunch.bunchify(yaml.load(f))


def init_log(name, console_level=20, file_level=20, filepath='.'):
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    
    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(console_level)
    # log.addHandler(console_handler)
    
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(name)-12s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    
    file_handler = TimedRotatingFileHandler(
                        os.path.join(filepath, name+'.log'), 
                        when='D', interval=1)
    file_handler.setLevel(file_level)
    log.addHandler(file_handler)

    return log

log = init_log('rq-workflow', console_level=10)

class Job(object):
    """

    Every task must define a run method and/or a requires method.  If a requires method 
    is defined and a run method is not, the task is essentially a wrapper for all task(s)
    returned in the requires method.
    """

    """TODO:Add a preprocessor method that's called in Job.__init__ 
           to replace having to override Job.__init__"""

    def __init__(self, redis_connection=None, queue=None, **kwargs):
        for k, v in kwargs.iteritems():
            assert k in self._allowed_parameters, "'{}' is an invalid parameter.".format(k)
            setattr(self, k, v)
        for k, v in self._get_job_parameters():
            # print k, v
            if k not in kwargs.keys():
                setattr(self, k, v)
        try:
            self._config = load_yaml_config('RQ_TASKS_CONFIG')
        except SystemExit:
            'TODO: self._config = default config'
            self._config = {}
        self.redis = redis_connection or self._config.get('redis', {}).get('connection') or StrictRedis()
        self.queue_name = queue or self._config.get('default_queue') or 'rq_tasks'
        self.queue = Queue(name=self.queue_name, connection=self.redis)
        self.dependencies = []

    def requires(self):
        """Return one or more task dependencies"""
        return None

    def run(self):
        """
        This method should return the function to be 
        enqueued or a tuple of the function, args and/or kwargs.

        """
        return None
    
    def output(self):
        """A value (e.g. filepath) to be accessible to any downstream tasks.

        This value is conceptually different from the return value of the function
        to be enqueued which is returned in the return method.
        """
        return None

    def input(self):
        """A helper method to access task dependencies"""
        if self.requires():
            return self.requires().output()
        else:
            return None

    def get_job(self):
        return self.queue.fetch_job(self._job_id)

    def enqueue(self):

        log.info('Checking job status: {}'.format(self._job_description))
        if self._is_enqueued:
            self.print_summary()
            return
        log.info('Enqueuing job.')

        if self.requires():
            log.info('Enqueuing dependencies.')
            for job in self.requires():
                job = job.enqueue()
                self.dependencies.append(job)

        if self.run():
            f, args, kwargs = self.run()
        else:
            f, args, kwargs = wrapper_task, None, None
        meta = {'job_name': self._name, 'job_parameters':self._parameters}
        rqjob = RQJob.create(f, args=args, kwargs=kwargs,
                   id=self._job_id, description=self._job_description,
                   connection=self.redis, origin=self.queue_name,
                   timeout=-1, result_ttl=-1, ttl=-1, meta=meta,
                   depends_on=self.dependencies)
        self.queue.enqueue_job(rqjob)
        # self.print_summary()
        return rqjob

    @property
    def _is_enqueued(self):
        return self.queue.fetch_job(self._job_id)

    def print_summary(self):
        # d:
        #     a:
        #         failed
        #         start
        #         deferred
        #     b: 
        #         failed
        #         start
        #         deferred

        cnt = collections.Counter()
        summary = bunch.Bunch()

        job = self.get_job()
        self.status = job.get_status()
        log.info('Job status: {}'.format(self.status))
        if self.status in ['queued', 'started', 'finished', 'deferred', 'failed']:
            'TODO: recursively walk through dependencies to create summary'
            dependencies = job._dependency_ids
            for d in dependencies:
                d = self.queue.fetch_job(d)
                job_name = d.meta['job_name']
                status = d.get_status()
                if job_name in summary:
                    summary[job_name][status] += 1
                else:
                    summary[job_name] = collections.Counter()
                    summary[job_name][status] += 1
        print bunch.bunchify(summary)

    @property
    def _job_id_full(self):
        return 'rq:job:' + self._job_id

    @property
    def _job_id(self):
        """Return the human-readable _job_id as a Redis memory-efficient hash.

        Questions:
            What's the most memory efficient way to store a redis key?
            
        Possible implementations:
            http://stackoverflow.com/questions/2511058/persistent-hashing-of-strings-in-python
        """
        return hashlib.md5(self._job_description).hexdigest()

    @property
    def _job_description(self):
        return self._name + self._parameters

    @property 
    def _parameters(self):
        parameters = self._get_job_instance_parameters()
        kwargs = []
        for k,v in parameters.items():
            if isinstance(v, str):
                v = "'{}'".format(v)
            kwargs.append('{}={}'.format(k, v))
        arg_list = sorted(kwargs)
        args = ', '.join(arg_list)
        return '({})'.format(args)

    @property 
    def _name(self):
        # module_name = os.path.basename(__file__).split('.')[0]

        job_name = self.__class__.__name__
        return '{}.{}'.format(self.__module__, job_name)
        # return self.__class__.__name__


    def _get_job_instance_parameters(self):
        return {k:getattr(self, k) for k in self._allowed_parameters}

    @property
    def _allowed_parameters(self):
        return [x[0] for x in self._get_job_parameters()]

    @classmethod
    def _get_job_parameters(cls):
        return [a for a in inspect.getmembers(cls, lambda a:not(inspect.isroutine(a))) 
                if not a[0].startswith('_')]
